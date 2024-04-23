""" Exécution de la requête à partir de la planification """
from __future__ import annotations
from typing import Optional, Callable
from collections.abc import Iterator
import logging
from sqlglot import exp

from boic import jewel as J, shards

from . import plan as P
from .filter import filter_cursor, generate_filter_func

logger = logging.getLogger(__name__)

class Context:
    def __init__(self, cursor: Optional[Iterator[any]] = None):
        self.cursor = cursor

class Cursor:
    def __next__(self):
        raise NotImplementedError("Un curseur doit être un itérateur.")

    def __iter__(self):
        return self

    def is_row_cursor(self):
        """ Retourne True si le curseur pointe sur une ligne quand itéré. """
        return isinstance(self, RowCursor)

class RowCursor(Cursor):
    """ Curseur qui lit ligne par ligne """

    def __init__(self, columns: list[P.ColumnProjection] = None):
        self.row = None
        self.columns = list(columns) if columns else None

    def keys(self):
        return list(map(lambda c: c.alias, self.columns[:]))

    def __contains__(self, alias: str) -> bool:
        return any(map(lambda _: True, filter(lambda col: col.alias == alias, self.columns)))

    def __getattr__(self, alias: str) -> any:
        return self[alias]

    def __getitem__(self, alias: str) -> any:
        col = next(filter(lambda col: col.alias == alias, self.columns))
        cid = self.columns.index(col)
        return self.row[cid]

class ShardCursor(RowCursor):
    """ Curseur qui scanne l'ensemble des Shards. 

        L'attribut *columns* retourne les clés du Shard sur lequel le curseur est placé. 
        Il n'y a donc pas de garantie de stabilité dessus, il est 
        préférable de sélectionner les données pour générer un curseur
        dont les colonnes sont garanties.
    """
    def __init__(self, shards: Iterator[shards.Shard]):
        super().__init__()
        self.shards = shards
        self.row = None
        self.columns = None

    def __next__(self):
        shard = next(self.shards)
        self.columns = list(
            map(
                lambda alias: P.PerAliasFetch(src_alias=alias, alias=alias),
                shard.keys()
            )
        )
        self.row = list(map(lambda c: c(shard), self.columns))
        return self

class ProjectCursor(RowCursor):
    """ Curseur réalisant une projection des données (par sous-sélection, ou par appel de fonction)
    
        Ce curseur ne permet pas des opérations de tris ou d'agrégation.
    """
    def __init__(self, columns: list[P.ColumnProjection], cursor: RowCursor):
        super().__init__(columns=columns)
        self.cursor = cursor

    def __next__(self):
        next(self.cursor)

        self.row = []
        
        for col in self.columns:
            self.row.append(col(self.cursor))
        
        return self

class FilterCursor(RowCursor):
    """ Curseur réalisant un filtre """
    def __init__(self, filter: Callable[[RowCursor], bool], cursor: RowCursor):
        self.filter = filter
        self.cursor = cursor

    def __next__(self):
        while not self.filter(next(self.cursor)): continue
        
        self.row = self.cursor.row[:]
        self.columns = self.cursor.columns[:]
        
        return self

class Execution:
    def __init__(self):
        self.cursors = {}

def execute_plan(jewel: J.Jewel, plan: P.Plan, max_depth: Optional[int] = None) -> Cursor:
    """ Execute le plan d'exécution, retourne un curseur à itérer. """
    execution = Execution()

    queue = set(plan.leaves())

    while queue:
        step = queue.pop()

        # Ouvre un curseur vers les Shards.
        if isinstance(step, P.OpenShardCursor):
            execution.cursors[step] = _open_shard_cursor(jewel, step, max_depth=max_depth)

        elif isinstance(step, P.Scan):
            execution.cursors[step] = _scan(jewel, execution, step)

        # Enfile les étapes dépendantes de celui qui vient d'être executé.
        queue.update(step.dependants)

    # Récupère l'étape racine
    root = plan.root
    
    # Retourne le curseur d'exécution.
    return execution.cursors[root]

def _open_shard_cursor(jewel: J.Jewel, step: P.OpenShardCursor, max_depth=None):
    """ Ouvre un curseur scannant l'ensemble des Shards.

        Si shard_type est défini, réalise un pré-filtre sur le paramètre "type" du Shard.
    """
    def cursor():
        for shard in shards.iter(jewel, max_depth=max_depth):
            if step.type:
                if shard.is_type(step.type):
                    yield shard
            else:
                yield shard

    return ShardCursor(shards=cursor())

def _scan(jewel: J.Jewel, execution: Execution, step: P.Scan) -> RowCursor:
    """ Scanne un ensemble à partir du curseur généré par la source """

    # Récupère le curseur de la source.
    cursor = execution.cursors[step.source]

    # Génère une fonction de projection de l'entrée.
    if step.project:
        cursor = ProjectCursor(columns=step.project.columns, cursor=cursor)
        
    # Filtre le curseur
    if step.condition:
        filter = generate_filter_func(step.condition)
        cursor = FilterCursor(filter=filter, cursor=cursor)

    return cursor


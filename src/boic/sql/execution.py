""" Exécution de la requête à partir de la planification """
from typing import Optional, Union
from collections.abc import Iterator

from boic import jewel as J, shards

from . import plan as P
from .filter import filter_cursor

class Context:
    def __init__(self, cursor: Optional[Iterator[any]] = None):
        self.cursor = cursor

class Execution:
    def __init__(self):
        self.contexts = {}


def execute_plan(jewel: J.Jewel, plan: P.Plan, max_depth: Optional[int] = None):
    """ Execute le plan d'exécution, retourne un curseur à itérer. """
    execution = Execution()

    queue = set(plan.leaves())
    contexts = {}

    while queue:
        step = queue.pop()

        # Ouvre un curseur vers les Shards.
        if isinstance(step, P.OpenShardCursor):
            execution.contexts[step] = _open_shard_cursor(jewel, step, max_depth=max_depth)

        elif isinstance(step, P.Scan):
            execution.contexts[step] = _scan(jewel, execution, step)

        # Enfile les étapes dépendantes de celui qui vient d'être executé.
        queue.update(step.dependants)

    # Récupère l'étape racine
    root = plan.root
    
    # Retourne le curseur d'exécution.
    return execution.contexts[root].cursor

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

    return Context(cursor=cursor())

def _scan(jewel: J.Jewel, execution: Execution, step: P.Scan) -> Context:
    """ Scanne un ensemble à partir du curseur généré par la source """

    # Récupère le curseur de la source.
    cursor = execution.contexts[step.source].cursor

    # Génère une fonction de transformation de l'entrée.
    # TODO: Implémenter la fonction de transformation.
    
    # Filtre le curseur
    cursor = filter_cursor(cursor, step.condition)

    ctx = Context()
    ctx.cursor = cursor

    return ctx


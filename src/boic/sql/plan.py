"""Planifie la requête ShQL

Le but est de générer le plan d'exécution à partir de l'AST de la requête SQL. 
"""
from __future__ import annotations
from sqlglot import exp
from typing import Optional, Union
from collections.abc import Iterator, Iterable

from boic.jewel import Jewel, JewelPath

class Plan:
    def __init__(self):
        # Permet de lier une étape à une alias
        self.step_aliases = {}
        # Compteur des idenfiants de l'étape
        self.step_counter = -1
        # Etapes
        self.steps = []
        # Racine
        self.root = None

    def new_id(self):
        self.step_counter += 1
        return self.step_counter

    def scan(self, name = None, condition=None, project=None, source=None, deps=None):
        return Scan(
            self,
            name=name,
            source=source,
            condition=condition,
            project=project,
            deps=deps
        )

    def open_shard_cursor(self, name: str, type = None):
        return OpenShardCursor(plan=self, type=type)

    def leaves(self):
        """ Retourne les feuilles de l'arbre de planification """
        return filter(Step.is_leave, self.steps)

    def __repr__(self):
        return "\n".join(
            [self.root.explain()] 
        )
class Step:
    def __init__(self, plan: Plan, name: Optional[str] = None, deps: Optional[list[Step]] = None):
        self.id = plan.new_id()
        plan.steps.append(self)

        self.name = name

        # Dépendances de l'étape
        self.dependencies = deps or []
        self.dependants = []

        # Enregistre les dépendants.
        for dep in self.dependencies:
            dep.dependants.append(self)

    def explain_spec(self, ident: int) -> str:
        return ""
    
    def explain(self, ident: int = 0) -> str:
        space = "  " * ident
        cspace = space + "  "

        fragments = [
            f"{type(self).__name__} #{self.id} (\n",
            *self.explain_spec(ident+1),
            cspace + f"deps=[{', '.join(map(lambda s: str(s.id), self.dependencies))}]\n",
            space + ')'
        ]
        return "".join(fragments)

    def __hash__(self):
        return self.id

    def is_leave(self):
        """ Vérifie si l'étape est une feuille dans l'arbre de planification """

        return len(self.dependencies) == 0
        
StepId = int

class Transform:
    """ Réalise une projection d'une ligne vers une autre """
    def __init__(self):
       pass

class OpenShardCursor(Step):
    """ Représente un curseur sur l'ensemble des Shards. """
    def __init__(self, plan: Plan, name: Optional[str] = None, type: Optional[str] = None):
        super().__init__(plan=plan, name=name)
        self.type = type

    def explain_spec(self, ident: int) -> str:
        space = "  " * ident
        return "".join([
            space + "type=",
            self.type,
            '\n'
        ])

class WriteNewShard(Step):
    """ Ecris un nouveau Shard dans le Jewel """
    def __init__(self, path: JewelPath, columns, values):
        self.path: JewelPath = values
        self.columns = {}
        
        for k, v in zip(columns, values):
            if k == "path":
                continue
            self.columns[k] = v

class Scan(Step):
    """ Scanne à partir d'un curseur sur une ligne, et applique des projections et/ou des filtres 
    
        Cela conduit à générer un curseur filtré puis projeté.
    """
    def __init__(self, plan: Plan, deps: Optional[list[Step]], name: Optional[str] = None, source: exp.Expression = None, condition: Optional[exp.Expression] = None, project: Optional[P.Projection] = None):
        super().__init__(plan=plan, deps=deps)
        self.source = source
        self.condition = condition
        self.project = project

    def explain_spec(self, ident: int) -> str:
        space = "  " * ident
        return "".join([
            (space + "source= " + self.source.explain(ident) + ',\n'),
            (space + "projection=" + self.project.explain(ident) + ',\n') if self.project else "",
        ])

class ColumnProjection:
    """ Projette une valeur depuis une ligne sur une colonne """
    def __init__(self, rank: int = None, alias: str = None):
        self.rank = rank
        self.alias = alias

    def __call__(self, source: RowCursor) -> any:
        raise NotImplementedError("")
    
    def explain(self, ident: int) -> str:
        if self.rank is not None:
            return f"{self.rank} ({self.alias}) := {self.explain_spec(ident + 1)}"
        else:
            return self.explain_spec(ident + 1)

class PerAliasFetch(ColumnProjection):
    """ Récupère la valeur du curseur source à l'alias passé en argument. """
    def __init__(self, src_alias: str, nested: Optional[Fetch] = None, alias: Optional[str] = None):
        super().__init__(alias=alias)
        self.src_alias = src_alias
        self.nested = nested
    
    def __call__(self, source: RowCursor):
        if self.nested:
            value = self.nested(source)
        else:
            value = source
        
        if not value or self.src_alias not in value:
            return None

        return value[self.src_alias]
    
    def explain_spec(self, ident: int) -> str:
        if self.nested:
            src = f"({self.nested.explain(ident)}).{self.src_alias}"
        
        else:
            src = self.src_alias
        
        return src

def _project_col(expr: exp.Expression) -> Projection:
    """ Implémenter les fonctions FUNC(arg0, ...) qui ne provoquent pas d'agrégation ou de tri. 
    """
    if isinstance(expr, exp.Column):
        src_alias, table = (str(expr.this.this), expr.table)
        return PerAliasFetch(src_alias=src_alias)

    elif isinstance(expr, exp.Dot):
        src_alias = str(expr.expression.this)
        nested = expr.this
        return PerAliasFetch(alias=src_alias, src_alias=src_alias, nested=_project_col(nested))
    
    else:
        raise NotImplementedError(f"La transformation de ligne n'implémente pas l'expression {type(expr)}")

class Projection:
    def __init__(self, columns: list[ColumnProjection]):
        
        for rank, col in enumerate(columns):
            col.rank = rank

        self.columns = columns
    
    def explain(self, ident: int) -> str:
        space = "  " * ident
        return "".join([
            "Projette (\n",
            *list(map(lambda col: space + "  " + col.explain(ident+1) + '\n', self.columns)),
            space + ')'
        ])

def _project(cols: list[exp.Expression]):
    """ Projette une ligne à partir d'une autre ligne """
    columns = []
        
    for expr in cols:
        alias = None

        if isinstance(expr, exp.Alias):
            alias = expr.alias
            expr = expr.this
        else:
            logger.warning("Aucun alias n'est définit pour la colonne.")

        col = _project_col(expr)
        col.alias = alias

        columns.append(col)
    
    return Projection(columns=columns)

def contains_wildcard(exprs: Iterable[exp.Expression]):
    """ Vérifie si la liste d'expressions contient un wildcard "*" """
    return any(map(lambda expr: isinstance(expr, exp.Star), exprs))

def generate_step(plan: Plan, node: exp.Expression) -> Step:
    """ Génère une étape dans l'exécution de la requête """
    
    if isinstance(node, exp.Select):
        # Deux possibilités :
        # - Select sur une sous-requête, dans ce cas la dép doit fournir un curseur à itérer.
        # - Select sur une table (Shard)
        _from = node.args.get("from")

        # Si on a pas passé de FROM, dans ce cas on ouvre un curseur sur l'ensemble des Shards par défaut.
        source = generate_step(plan, _from) if _from else plan.open_shard_cursor(name="shard", type="shard")

        # On scanne le sous-ensemble à partir de la source.
        step = plan.scan(source=source, deps=[source])
        
        # On transforme la ligne, sauf si on a un wildcard (*)
        if not contains_wildcard(node.expressions):
            step.project = _project(node.expressions)

        where = node.args.get("where")
        
        if where:
            # TODO: Implémenter la possibilité de shifter du 
            # ScanShards -> FetchShards(condition=where, cursor=IntersectIndexes(...FetchIndex(per_idx_condition)))$
            # Cette opération permettant de profiter des éventuels index crées pour accélérer la requête.
            step.condition = where.this      

    elif isinstance(node, exp.From):
        if isinstance(node.this, exp.Table):
            table = node.this
            shard_type = table.this.this
            alias = table.alias
        else:
            raise ValueError(f"{type(node)}")

        step = plan.open_shard_cursor(name=alias, type=shard_type)

    elif isinstance(node, exp.Insert):
        schema = node.this
        table = schema.this
        columns = schema.expressions
        values = node.expression.expressions
        shard_type = table.this.this

    else:
        raise NotImplementedError(f"L'expression de type {type(node)} n'est pas implémentée pour la planification de l'execution de la requête")

    return step

def generate_plan(node: exp.Expression):
    """ Génère le plan d'exécution à partir de l'AST de la requête ShQL """
    plan = Plan()
    plan.root = generate_step(plan, node)
    return plan

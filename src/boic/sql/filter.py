from typing import Generator, Callable, TypeVar, Optional
from collections.abc import Iterator
import re

from sqlglot import parse_one, exp
from sqlglot.planner import Plan, Scan, Aggregate, Join, Sort, SetOperation
from sqlglot.optimizer import optimize

from boic.shards import Shard, ShardValue
from boic import shards, jewel as J

from .eval import eval_expr

CursorFilterCallable = Callable[[dict], bool]

def _like(lhs: exp.Expression, rhs: exp.Expression) -> CursorFilterCallable:
    """ Génère une fonction python executant l'opération VALUE LIKE 'PATTERN' """

    def func(row: dict) -> bool:
        pattern = re.compile(eval_expr(row, rhs).replace('%', '.*'))
        val = eval_expr(row, lhs)

        if not val:
            return False

        if isinstance(val, ShardValue):
            if val.is_string():
                val = str(val)
            else:
                return False

        return pattern.match(val)

    return func

def _eq(lhs: exp.Expression, rhs: exp.Expression) -> CursorFilterCallable:
    
    def func(row: dict) -> bool:
        lh = eval_expr(row, lhs)
        rh = eval_expr(row, rhs)

        return lh == rh

    return func

def _and(lhs: exp.Expression, rhs: exp.Expression) -> CursorFilterCallable:
    lhs = _gen_filter_func(lhs)
    rhs = _gen_filter_func(rhs)

    def func(row: dict) -> bool:
        return lhs(row) and rhs(row)

    return func

def _or(lhs: exp.Expression, rhs: exp.Expression) -> CursorFilterCallable:
    lhs = _gen_filter_func(lhs)
    rhs = _gen_filter_func(rhs)

    def func(row: dict) -> bool:
        return lhs(row) or rhs(row)

    return func

def generate_filter_func(expr: exp.Expression) -> CursorFilterCallable:
    if isinstance(expr, exp.Like):
        return _like(expr.this, expr.expression)
    
    elif isinstance(expr, exp.And):
        return _and(expr.this, expr.expression)

    elif isinstance(expr, exp.Or):
        return _and(expr.this, expr.expression)

    elif isinstance(expr, exp.EQ):
        return _eq(expr.this, expr.expression)

    else:
        raise ValueError(f"Unimplemented type {type(expr)} for cursor filtering.")

Row = TypeVar('Row')

def filter_cursor(cursor: Iterator[Row], condition: Optional[exp.Expression]) -> Iterator[Row]:
    """ Filtre le curseur """

    if not condition:
        return cursor

    return filter(generate_filter_func(condition), cursor)
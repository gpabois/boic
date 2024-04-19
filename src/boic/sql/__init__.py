from typing import Generator
import re
import logging

from sqlglot import parse_one, exp
from sqlglot.planner import Plan, Scan, Aggregate, Join, Sort, SetOperation
from sqlglot.optimizer import optimize

from boic.shards import Shard
from boic import shards, jewel as J

from .filter import filter_cursor
from .plan import generate_plan
from .execution import execute_plan

logging.getLogger(__name__)

def execute(jewel: J.Jewel, query: str, max_depth=None):
    """ Execute la requête ShQL (Shard Query Language, un sous-ensemble du SQL), et retourne un curseur. """
    ast = parse_one(query)
    optimized = optimize(ast)

    logging.debug(f"Requête: {query}")
    logging.debug(f"AST: {repr(optimized)}")
    plan = generate_plan(optimized)
    logging.debug(f"Plan d'exécution: {repr(plan)}")
    return execute_plan(jewel, plan, max_depth=max_depth)



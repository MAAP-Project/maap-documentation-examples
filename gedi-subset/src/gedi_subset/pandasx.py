from collections import defaultdict

from pandas.core.computation.expr import Expr
from pandas.core.computation.scope import ensure_scope


def parse_expr(expr: str, *, global_dict=None, local_dict=None) -> Expr:
    resolver = defaultdict(int, __foo__=0)
    env = ensure_scope(0, global_dict or {}, local_dict or {}, resolvers=(resolver,))

    return Expr(expr, env=env)

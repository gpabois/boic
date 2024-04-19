from sqlglot import exp

def eval_expr(row: dict, expr: exp.Expression) -> any:
    """ Evalue une expression et retourne une valeur. """

    if isinstance(expr, exp.Column):
        key = eval_expr(row, expr.this)
        
        if key not in row:
            return None

        return row[eval_expr(row, expr.this)]
    
    elif isinstance(expr, exp.Identifier):
        return expr.this
    
    elif isinstance(expr, exp.Literal):
        return expr.this

    else:
        raise ValueError(f"Unimplemented type: {type(expr)} for value evaluation.")
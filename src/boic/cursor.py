
class Cursor:
    def __next__(self):
        raise NotImplementedError("Un curseur doit être un itérateur.")

    def __iter__(self):
        return self

    def is_row_cursor(self):
        """ Retourne True si le curseur pointe sur une ligne quand itéré. """
        return isinstance(self, RowCursor)


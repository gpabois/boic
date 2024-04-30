from collections.abc import Iterator, Literal
import json
from boic.jewel import Jewel

IndexType = Literal["flatten"]

class Schema:
    """ Schéma d'un index. """
    def __init__(self, name: str, type: IndexType, columns):
        self.name = name
        self.type = type
        self.columns = columns

class Index:
    def __init__(self, schema: Schema):
        self.shema = schema

    def __iter__(self) -> Iterator[IndexCursor]:
        raise NotImplementedError("L'index doit implémenter __iter__ pour être scanné en intégralité.")

class IndexCursor:
    def __init__(self, columns: list[str], values: list[any])
        self.columns = columns
        self.values = values

    def __getitem__(self, alias: str) -> any:
        col_id = self.columns.index(alias)
        return self.values[col_id]

class Flatten(Index):   
    """ Liste plate """
    def __init__(self, jewel: Jewel, schema: Schema):
        super().__init__(schema=schema)
        self.jewel = jewel
        self.stream = stream
    
    def __iter__(self) -> Iterator[IndexCursor]
        with self.jewel.path(jewel.config.index.dir, self.schema.name).open(mode="r") as file:
            for line in file:
                yield IndexCursor(columns=self.columns, values=line.split(";"))

class IndexManager:
    def __init__(self, jewel: Jewel):
        self.jewel = jewel
        self.schema = self.load_schemas()

    def __iter__(self) -> Iterator[Schema]:
        return self.schema.values()

    def load_schemas(self) -> dict[str, Schema]:
        schemas_loc = self.jewel.path(jewel.config.index.dir, "schemas")
        
        if not schemas_loc.exists():
            return {}

        with schemas_loc.open(mode="r") as file:
            ser_schemas = json.load()
        
        schemas = {}

        for _, ser_schema in
            schema = Schema(id=ser_schema["id"], type=ser_schema["type"], columns=ser_schema["columns"])
            schemas[schema.id] = self.load_index_from_schema(schema)

        return schemas

    def load_index_from_schema(self, schema: Schema) -> Index:
        if schema.type == "flatten":
            return Flatten(schema)
        else:
            raise ValueError(f"Type d'index {schema.type} inconnu.")

    def flush_schemas(self):
        schemas = {}

        for schema in self.schema.values():
            schemas[schema.name] = {
                'id': schema.id,
                'type': schema.type,
                'columns': schema.columns
            }

        with self.jewel.path(jewel.config.index.dir, "schemas").open(mode="w") as file:
            file.write(json.dumps(schema))

    def new(id: str, type: IndexType, columns: list[str]):
        if id in self.schemas:
            raise ValueError(f"Un index avec l'identifiant {id} existe déjà.")

    def _index_loc(self, index_name: str) -> JewelPath:
        return self.jewel.path(self.jewel.config.index.dir, index_name)

    def __contains__(self, id: str) -> bool:
        return id in self.schemas

    def __getitem__(self, id: str) -> Optional[Index]
        return self.schemas[id]
        






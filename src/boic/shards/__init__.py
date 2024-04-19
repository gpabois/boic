from __future__ import annotations
import json
import markdown
import frontmatter
from boic.jewel import Jewel, JewelPath
from enum import Enum

class ShardType(Enum):
    AIOT = "AIOT"

class ShardValue:
    def __init__(self, jewel: Jewel, value: any):
        self.jewel = jewel
        self.value = value
        self.cached_shard = None

    def __getitem__(self, key: str):
        if isinstance(self.value, dict):
            return self.value[key]
        elif self.is_shard_uri():
            if not self.cached_shard:
                self.cached_shard = self.get_shard()
            return self.cached_shard[key]
        else:
            raise ValueError("Not a shard or a dictionnary")
    
    def keys(self):
        if isinstance(self.value, dict):
            return self.value.keys()
        elif self.is_shard_uri(self):
            if not self.cached_shard:
                self.cached_shard = self.get_shard()
            return self.cached_shard.keys()   
        else:
            raise ValueError("Not a shard or a dictionnary")

    def lower(self):
        return self.value.lower()
    
    def upper(self):
        return self.value.upper()

    def get_shard(self) -> Shard | None:
        path = JewelPath.from_jewel_uri(self.jewel, self.value)
        return Shard.read(path)
    
    def is_string(self):
        return isinstance(self.value, str)
    
    def is_shard_uri(self):
        return isinstance(self.value, str) and self.value.startswith("jewel://")

    def __str__(self):
        if isinstance(self.value, dict) or isinstance(self.value, list):
            return json.dumps(self.value, indent=2)
        
        return str(self.value)

class Shard:
    def __init__(self, path: JewelPath, content: str, meta):
        self.content = content
        self.meta = {k: ShardValue(path.jewel, v) for k, v in meta.items()}
        self.path = path

    def __str__(self):
        return str(self.path)
        
    def __getitem__(self, key: str) -> any:
        if key == "id":
            return self.path.rel
        
        if key  == "path":
            return self.path

        return self.meta[key]

    def __contains__(self, key: str) -> bool:
        return key in self.meta

    def __setitem__(self, key: str, value: any):
        self.meta[key] = value

    def __repr__(self) -> str:
        args = [f"{key}={self[key]}" for key in self.keys()]
        typ = self["type"] if "type" in self else "Shard"
        return f"{typ}({', '.join(args)})"

    @staticmethod
    def read(path: JewelPath) -> Shard:
        with path.open(mode="r") as file:
            content = file.read()
            meta, content = frontmatter.parse(content)
            md = markdown.Markdown(extensions = ['meta'])
            md.convert(content)
            return Shard(path, content, meta)

    def as_template_var(self) -> dict:
        """
            Enrobe l'éclat en tant que variable pour modèle dont la clé est le type.

            Exemple: {"AIOT": {"code_aiot: ...}}
        """
        if 'type' in self:
            return {self['type']: {**self.meta}}
        else:
            return {**self.meta}

    def as_row(self) -> dict:
        return {**self.meta}

    def keys(self):
        return list(self.meta.keys()) + ["path", "id"]
    
    def items(self):
        for k in self.keys():
            yield (k, self[k])

    def is_type(self, typ: str) -> bool:
        if typ == "shard":
            return True
            
        return "type" in self.keys() and self["type"].lower().startswith(typ.lower())

def build_primary_index(jewel: Jewel, max_depth=None):
    """ Construit l'index primaire des Shards"""
    
    pass

def get_primary_index(jewel: Jewel) -> BPlusTree:
    """ Récupère un index à partir de son nom """
    
    return None
    
def scan_shards(jewel: Jewel, max_depth=None):
    """ Itère en parcourant l'ensemble du Jewel """
    for _root, _dirs, files in jewel.root().walk(max_depth=max_depth):
        for file in files:
            if file.suffixes and file.suffixes[-1] == ".md":
                shard = Shard.read(file)
                yield shard

def iter_by_primary_index(jewel: Jewel, max_depth=None):
    """ Itère en partant de l'index primaire de Shards """
    index = get_primary_index(jewel)
    
    # Par défaut, on replie sur une itération brute.
    if index is None:
        yield from scan_shards(jewel, max_depth=max_depth)
        return

    for path in index.values():
        shard_path = jewel.root().join(path.decode())
        yield Shard.read(shard_path)

    indexes.close()

def iter(jewel: Jewel, max_depth=None, skip_indexes=False) -> Iterator[Shard]:
    """
        Itère sur l'ensemble des fragments en partant de la racine.
    """
    if skip_indexes:
        yield from scan_shards(jewel, max_depth=max_depth)
    
    yield from iter_by_primary_index(jewel, max_depth=max_depth)
    



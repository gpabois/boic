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
            return ShardValue(jewel=self.jewel, value=self.value[key])
        elif isinstance(self.value, list):
            return ShardValue(jewel=self.jewel, value=self.value[key])
        elif self.is_shard_uri():
            if not self.cached_shard:
                self.cached_shard = self.get_shard()
            return self.cached_shard[key]
        else:
            raise ValueError(f"Not a shard or a dictionnary to access property {key}")
    
    def __contains__(self, key: str):
        if isinstance(self.value, dict):
            return key in self.value
        elif self.is_shard_uri():
            if not self.cached_shard:
                self.cached_shard = self.get_shard()
            return key in self.cached_shard
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

    def get_shard(self) -> Shard:
        """ Charge le Shard si la valeur est un chemin absolu ou un Jewel URI"""
        path = self.jewel.path(self.value)
        return Shard.load(path)
    
    def is_string(self):
        return isinstance(self.value, str)
    
    def is_shard_uri(self):
        return isinstance(self.value, str) and self.value.startswith("jewel://")

    def __str__(self) -> str:
        import yaml
        
        if isinstance(self.value, dict) or isinstance(self.value, list):
            return yaml.dump(self.value, default_flow_style=False)
        
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
            return "/".join(self.path.segments)
        
        if key  == "path":
            return self.path

        return self.meta[key]

    def __getattr__(self, key: str) -> any:
        return self[key]

    def __contains__(self, key: str) -> bool:
        return key in self.meta

    def __setitem__(self, key: str, value: any):
        self.meta[key] = value

    def __repr__(self) -> str:
        args = [f"{key}={self[key]}" for key in self.keys()]
        typ = self["type"] if "type" in self else "Shard"
        return f"{typ}({', '.join(args)})"

    @staticmethod
    def load(path: JewelPath) -> Shard:
        with path.open(mode="r") as file:
            content = file.read()
            meta, content = frontmatter.parse(content)
            md = markdown.Markdown(extensions = ['meta'])
            md.convert(content)
            return Shard(path, content, meta)

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
                shard = Shard.load(file)
                yield shard

def iter_by_primary_index(jewel: Jewel, max_depth=None):
    """ Itère en partant de l'index primaire de Shards """
    index = get_primary_index(jewel)
    
    # Par défaut, on replie sur une itération brute.
    if index is None:
        yield from scan_shards(jewel, max_depth=max_depth)
        return

    for path in index.values():
        shard_path = jewel.path(path.decode())
        yield Shard.load(shard_path)

    indexes.close()

def iter(jewel: Jewel, max_depth=None, skip_indexes=False) -> Iterator[Shard]:
    """Itère sur l'ensemble des fragments en partant de la racine.
    """
    if skip_indexes:
        yield from scan_shards(jewel, max_depth=max_depth)
    
    yield from iter_by_primary_index(jewel, max_depth=max_depth)
    

def load(path: JewelPath) -> Shard:
    """ Charge un Shard à partir de son chemin """
    return Shard.load(path)

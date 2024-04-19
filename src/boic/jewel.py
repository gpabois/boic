from __future__ import annotations
import pathlib
import os
import logging

logger = logging.getLogger(__name__)

class Jewel:
    def __init__(self, root: str):
        if root is None:
            raise ValueError("Le chemin vers le jewel n'est pas définie.")
        self._root = pathlib.Path(root)
        self.config = {
            'templates': {
                "dir": {
                    'shards': "Modèles/Shards",
                    'documents': "Modèles/Documents"
                }
                "inspection": "INSPECTION-0.0.1",
                "aiot": "AIOT-0.0.1"
            },
            'equipe': {
                # Chemin vers le répertoire de l'équipe.
                'dir': "Equipe"
            },
            'aiot': {
                # Structure du dossier
                'dir': {
                    'inspections': '02_inspections'
                }
            }
        }
        self.load_configuration()
    
    def load_configuration():
        from yaml import loads, dump
        from mergedeep import merge

        conf = self.root().join("jewel.yml")
        if conf.exists():
            conf = loads(conf.open(mode="r"))
            self.config = merge({}, self.config, conf)
            
    def root(self) -> JewelPath:
        """ Lien vers la racine du Jewel """
        return JewelPath(self, "/")

class JewelPath:
    """ Un chemin vers une ressource dans un bijou."""
    
    canon: pathlib.Path

    @staticmethod
    def from_jewel_uri(jewel: Jewel, uri: str) -> JewelPath:
        rel = uri.removeprefix("jewel://")
        return JewelPath(jewel, rel)
        
    def __str__(self):
        return f"jewel://{str(self.rel)}"

    def __repr__(self):
        return f"JewelPath(path={self.rel}, canonical={self.canon})"

    def __fspath__(self):
        return self.canonicalize().__fspath__()

    def canonicalize(self) -> pathlib.Path:
        """ Retourne le lien canonique vers le fichier ou répertoire 
        """
        if self.canon:
            return self.canon

        path = self.jewel._root
        segments = list(filter(lambda s: s, self.rel.split("/")))
        
        while segments:
            segment = segments.pop(0)
            # Lien symbolique
            if segments and not path.joinpath(segment).exists() and path.joinpath(f"{segment}.jlnk").is_file():
                path = pathlib.Path(path.parent).joinpath(path.joinpath(f"{segment}.jlnk").open(mode='r').readline())
            elif segments and path.is_file():
                raise ValueError("Expecting a directory, or a jewel symbolic link.")
            else:
                path = path.joinpath(segment)

        self.canon = pathlib.Path(path).resolve()
        
        return self.canon

    def __init__(self, jewel: Jewel, rel: str):
        self.jewel = jewel
        self.rel = rel
        self.segments = list(filter(lambda s: s != "", self.rel.split("/")))
        
        if self.rel.startswith("/"):
            self.segments = ['', *self.segments]

        self.stem = None
        self.suffixes = self.segments[-1].split(".") if self.segments else []
        
        if self.suffixes:
            self.stem = self.suffixes.pop(0)
            self.suffixes = list(map(lambda s: f".{s}", self.suffixes))
        
        self.suffix = self.suffixes[-1] if self.suffixes else None

        # Cache le lien canonique.
        self.canon = None

    def is_file(self) -> bool:
        return self.canonicalize().is_file()

    def is_symlink(self) -> bool:
        return self.suffix == ".jlnk" and self.is_file() 

    def is_dir(self) -> bool:
        return os.path.isdir(self.canonicalize())

    def exists(self) -> bool:
        return os.path.exists(self.canonicalize())

    def mkdir(self, **kwargs):
        return self.canonicalize().mkdir(parents=True, exist_ok=True)

    def follow(self) -> JewelPath:
        """ Suit le lien symbolique """
        lnk = self.open().readline()
        segment = self.stem
        segments = self.rel.split("/")
        segments[-1] = segment
        rel = "/".join(segments)
        p = JewelPath(self.jewel, rel)
        p.canon = self.canon.parent.joinpath(lnk).resolve(strict=True)
        return p

    def join(self, *paths) -> JewelPath:
        rel = "/".join(self.segments + list(paths))
        return JewelPath(self.jewel, rel)

    def parent(self) -> JewelPath:
        rel = "/".join(self.segments[:-1])
        return JewelPath(self.jewel, rel)

    def walk(self, max_depth=None) -> Generator[JewelPath, None, None]:
        stack = [(self, 0, None)]
        while stack:
            p, d,typ = stack.pop(-1)

            if max_depth and d > max_depth:
                continue

            if typ is None:
                if p.is_symlink():
                    typ = "symlink"
                elif p.is_dir(): 
                    typ = "dir"
            
            if typ == "symlink":
                stack += [(p.follow(), d, None)]
                continue
            
            if typ == "dir":
                root = p
                dirs = []
                files = []
                
                try:
                    for entry in os.scandir(p.canonicalize()):
                        c = p.join(entry.name)
                        c.canon = pathlib.Path(entry.path)

                        if entry.is_file() and c.suffix == ".jlnk":
                            stack += [(c, d + 1, "symlink")]
                            files.append(c)
                        
                        elif entry.is_file():
                            files.append(c)

                        elif entry.is_dir():
                            stack += [(c, d + 1, "dir")]
                            dirs.append(c)
                    
                    yield (root, dirs, files)
                except Exception as e:
                    logger.debug(f"{e} ({p})")


    def open(self, **kwargs):
        return self.canonicalize().open(encoding="utf8", **kwargs)

def open(path: pathlib.Path) -> Jewel:
    return Jewel(path)

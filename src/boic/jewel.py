from __future__ import annotations
from collections.abc import Iterator
import itertools
import pathlib
import os
import logging

logger = logging.getLogger(__name__)

class JewelConfig:
    def __init__(self, **values):
        self.values = values
    
    def keys(self):
        return self.values.keys()

    def items(self):
        return self.values.items()

    def __getitem__(self, key: str):
        value = self.values[key]
        
        if isinstance(value, dict):
            return JewelConfig(**value)
        
        return value

    def __getattr__(self, key: str) -> JewelConfig | any:
        return self[key]

class Jewel:
    def __init__(self, root: str):
        if root is None:
            raise ValueError("Le chemin vers le jewel n'est pas définie.")
        self._root = pathlib.Path(root)
        self.config = JewelConfig(**{
            'templates': {
                "dir": {
                    'shards': "Modèles/Shards",
                    'documents': "Modèles/Documents"
                },
                "inspection": "INSPECTION-0.0.1",
                "aiot": "AIOT-0.0.1"
            },
            'equipe': {
                # Chemin vers le répertoire de l'équipe.
                'dir': "Equipe"
            },
            'aiot': {
                # Structure du dossier AIOT
                'dir': {
                    'archive': '00_archives',
                    'reglementation': '01_réglementation',
                    'inspection': '02_inspections',
                    'sanctions': '03_sanctions_contentieux',
                    'concertation': '04_concertation',
                    'urbanisme': '05_urbanisme',
                    'exploitant': '06_docs_exploitant',
                    'presentation': '07_presentations',
                    'workflow': '08_RVAT',
                    'misc': '09_Autres'
                }
            }
        })

        self.load_configuration()
    
    def load_configuration(self):
        from yaml import load, dump
        from mergedeep import merge

        conf = self.path("jewel.yml")
        if conf.exists():
            conf = loads(conf.open(mode="r"))
            self.config = JewelConfig(**merge({}, self.config, conf))
            
    def root(self) -> JewelPath:
        """ Lien vers la racine du Jewel """
        return JewelPath(self, [''])

    def path(self, *path: list[str|JewelPath]) -> JewelPath:
        return self.root().join(*path)

class JewelPath:
    """ Un chemin vers une ressource dans un bijou."""
    
    canon: pathlib.Path

    @staticmethod
    def from_str(jewel: Jewel, path: str) -> JewelPath:
        if path.startswith("jewel://"):
            return JewelPath.from_jewel_uri(jewel, path)
        
        elif path == "/":
            segments = [""]
        
        else:
            segments = path.split("/")
        
        return JewelPath(jewel, segments)

    @staticmethod
    def from_jewel_uri(jewel: Jewel, uri: str) -> JewelPath:
        path = uri.removeprefix("jewel://")
        return JewelPath.from_str(jewel, path)
    
    @staticmethod
    def is_jewel_uri(uri: str) -> bool:
        return uri.startswith("jewel://")
   
    @staticmethod
    def ensure(jewel: Jewel, path: str | JewelPath) -> JewelPath:
        if isinstance(path, str):
            return JewelPath.from_str(jewel, path)
        if isinstance(path, JewlePath):
            return path
        
        raise TypeError(f"Type incompatible pour être un jewel path ({type(path)})")

    def __str__(self):
        return f"jewel://{'/'.join(self.segments)}"

    def __repr__(self):
        return f"JewelPath(path={str(self)})"

    def __fspath__(self):
        return self.canonicalize().__fspath__()

    def canonicalize(self) -> pathlib.Path:
        """ Retourne le lien canonique vers le fichier ou répertoire 
        """
        if self.canon:
            return self.canon

        path = self.jewel._root
        segments = self.segments[:]
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

    def __init__(self, jewel: Jewel, segments: Iterator[str]):
        self.jewel = jewel
        self.segments = list(segments)      

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
        """ Crée le ou les repertoires """
        return self.canonicalize().mkdir(parents=True, exist_ok=True)

    def follow(self) -> JewelPath:
        """ Suit le lien symbolique """
        lnk = self.open().readline()
        path = self.parent().join(self.stem)
        path.canon = self.canonicalize().parent.joinpath(lnk).resolve(strict=True)
        return path

    def join(self, *paths) -> JewelPath:
        # Joint les segments
        segments = itertools.chain.from_iterable(
            itertools.chain(
                [self.segments],
                map(lambda path: path.segments, 
                    map(lambda path: JewelPath.ensure(self.jewel, path), paths)
                )
            )
        )

        return JewelPath(self.jewel, segments)

    def parent(self) -> JewelPath:
        return JewelPath(self.jewel, self.segments[:-1])

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

from __future__ import annotations
import argparse
import logging
import pathlib
import sys
import yaml
import os
from datetime import datetime
from typing import Optional
from boic import __version__, jewel as J, shards, templates, sql, gun
from prettytable import PrettyTable

__author__ = "G. PABOIS"
__copyright__ = "G. PABOIS"
__license__ = "MIT"

_logger = logging.getLogger(__name__)
_env_jewel_path = pathlib.Path(os.environ['JEWEL_PATH']) if 'JEWEL_PATH' in os.environ else None

def read_aiot(jewel: J.Jewel, max_depth=None) -> Optional[shards.Shard]:
    nom = input("AIOT: ")
    if nom.startswith("jewel://"):
        aiot = shards.load(jewel.path(nom))
        return aiot
    else:
        print(f"Recherche des candidats pour \"{nom}\"...")
        aiots = list(sql.execute(
            jewel, 
            f"SELECT * FROM aiot WHERE nom LIKE '%{nom}%'", 
            max_depth=max_depth
        ))

        if len(aiots) == 1:
            aiot = aiots[0]
            return aiot
        elif len(aiots) > 1:
            print("Les AIOTS suivants ont été trouvés : ")
            for i, candidate in enumerate(candidates, start=1):
                print(f"    {i}. {candidate['nom']}")
            i = input(f"Choisir entre {1}..{len(candidates)}: ") - 1
            aiot = aiots[i]
            return aiot
        else:
            print("Aucun candidat n'a été trouvé...")
            return None


def read_query():
    """ Lit une requête à partir du standard input """
    lines = []
    line = input("Veuillez taper la requête SQL (Shard Query Language):\n").strip()

    while not line.endswith(';'):
        lines.append(line)
        line = input().strip()

    lines.append(line)
    query = " ".join(lines)
    return query

# --- COMMANDS HANDLERS ---
def new_aiot(jewel: J.Jewel, args):
    print("-- Créer un nouvel  AIOT --")
    
    nom = input("Nom de l'AIOT: ")

    if "y" == input("Créer le dossier ? y/N (par défaut N): "):
        creer_dossier = True
    else:
        creer_dossier = False

    chemin = input("Chemin vers l'AIOT: ")
    
    numero = {
        'aiot': input("Numéro d'AIOT (par défaut vide): "),
        'dossier': input("Numéro de dossier (par défaut vide): "),
        'gup': input("Numéro GUP (par défaut vide): "),
    }

    commune = input("Code commune: ")
    
    inspecteur_id = input("Inspecteur en charge (Abbrévation): ").strip()
    if inspecteur_id:
        inspecteur_path = jewel.path(jewel.config.equipe.dir).join(f"{inspecteur_id}.md")
        inspecteur = shards.load(inspecteur_path)
    else:
        inspecteur = None

    print(f"Création de l'AIOT à {chemin}...")

    if creer_dossier:
        for _, dirname in jewel.config.aiot.dir.items():
            dir_path = jewel.path(chemin, dirname)
            print(f"Création du dossier: {dir_path}")
            dir_path.mkdir()

    fiche_path = jewel.path(chemin, "Fiche.md")
    templates.new_shard_template(
        jewel,
        jewel.config.templates.aiot,
        fiche_path,
        nom=nom,
        numero=numero,
        inspecteur=inspecteur,
        commune=commune
    )

def sync_aiot(jewel: J.Jewel, args):
    """ Synchronise le Shard avec les données GUN """
    aiot = read_aiot(jewel)
    gun_id = aiot.gun
    if gun_id is None:
        raise ValueError("L'AIOT ne comporte pas d'identifiant interne GUN.")
    gun.extract_situation_administrative(gun_id)

def new_inspection(jewel: J.Jewel, args):
    print("-- Créer une nouvelle inspection --")
    nom = input("Nom de l'AIOT: ")

    if nom.startswith("jewel://"):
        aiot = shards.load(jewel.path(nom))
    else:
        print(f"Recherche des candidats pour \"{nom}\"...")
        aiots = list(sql.execute(
            jewel, 
            f"SELECT * FROM aiot WHERE nom LIKE '%{nom}%'", 
            max_depth=args.max_depth
        ))

        if len(aiots) == 1:
            aiot = aiots[0]
        
        elif len(aiots) > 1:
            print("Les AIOTS suivants ont été trouvés : ")
            for i, candidate in enumerate(candidates, start=1):
                print(f"    {i}. {candidate['nom']}")
            i = input(f"Choisir entre {1}..{len(candidates)}: ") - 1
            aiot = aiots[i]
        
        else:
            print("Aucun candidat n'a été trouvé...")
            return 

    aiot_root_path = aiot.path.parent()
    print(f"Sélectionné: {aiot.nom} ({aiot_root_path})")

    nom = input("Nom de l'inspection: ")
    inspecteur_id = input("Inspecteur en charge (Abbrévation): ").strip()
    if inspecteur_id != "":
        inspecteur_path = jewel.path(jewel.config.equipe.dir, f"{inspecteur_id}.md")
        inspecteur = shards.load(inspecteur_path)
    else:
        raise ValueError("Un inspecteur doit être définit.")
    
    if inspecteur:
        print(f"Inspecteur: {inspecteur.nom} {inspecteur.prenom}")

    now = datetime.today()
    date_inspection = input("Date de l'inspection au format XX/XX/XX (par défaut: '{}'): ".format(now.strftime("%d/%m/%y")))
    
    if date_inspection.strip() == "":
        date_inspection = now
    
    else:
        date_inspection = datetime.strptime("%d/%m/%y")

    tags = input("Tags (séparé par , par défaut vide): ")
    
    if tags.strip() == "":
        tags = []
    
    else:
        tags = list(map(lambda tag: tag.strip(), tags.split(",")))

    nom_affaire = "{}_{}".format(date_inspection.strftime("%y%m%d"), nom)
    chemin_dossier_affaire = chemin_racine_aiot.join("02_inspections", date_inspection.strftime("%Y"), nom_affaire)

    default_template = "INSPECTION"
    print(f"Dossier de l'affaire: {chemin_dossier_affaire}")

    print("Création du dossier...")
    chemin_dossier_affaire.mkdir()
    chemin_dossier_affaire.join("PRE-INSP").mkdir()
    chemin_dossier_affaire.join("POST-INSP").mkdir()

    tags = map(lambda t: f'\"{t}\"', tags)
    templates.new_shard_template(
        jewel,
        default_template, 
        chemin_dossier_affaire.join(f"{nom_affaire}.md"),
        aiot=candidate,
        inspecteur=inspecteur,
        equipe=inspecteur.equipe,
        gun="",
        date_inspection=date_inspection.strftime("%y%m%d"),
        tags= f'[{", ".join(tags)}]'
    )

    print(f"Dossier disponible ici: {chemin_dossier_affaire.canonicalize()}")
    
def build_primary_index(jewel: J.Jewel, args):
    _logger.info("Construit l'index primaire...")
    shards.build_primary_index(jewel, max_depth=args.max_depth)
    _logger.info("Terminé !")

def execute_query(jewel: J.Jewel, args):
    query = read_query()

    _logger.info("Execution de la requête...")
    cursor = sql.execute(jewel, query, max_depth=args.max_depth)
    
    # C'est un curseur qui itère sur des lignes. 
    if cursor.is_row_cursor():  
        columns, rows = ([], [])

        for i, cursor in enumerate(cursor):
            if i == 0:
                columns = cursor.keys()
            
            row = []
            
            for col in columns:
                if col not in cursor:
                    row.append("N/D")
                else:
                    row.append(str(cursor[col]))
            
            rows.append(row)

        table = PrettyTable()
        table.align = "l"
        table.preserve_internal_border = True
        table.field_names = columns
        table.add_rows(rows)
        print(table)

def liste_aiots(jewel: J.Jewel, args):
    for shard in shards.iter(jewel, max_depth=args.max_depth):
        print(repr(shard))

def genere_modele_shard(jewel: J.Jewel, args):
    name = args.name
    output = jewel.root().join(args.output)
    context = read_context_files(jewel, args.context_files)
    templates.new_shard_template(jewel, name, output)

def genere_doc(jewel: J.Jewel, args):
    shard_path = jewel.root().join(args.shard)
    docx_path = shard_path.parent().join(f"{shard_path.stem}.docx")
    print(f"Génère un document à partir du shard : {shard_path}, vers: {docx_path}")
    shard = shards.load(shard_path)
    templates.new_docx_template(jewel, shard['template'], docx_path, **shard)

_commands = {
    'nouveau:inspection': new_inspection,
    'nouveau:aiot': new_aiot,
    'sync:aiot': sync_aiot,
    'genere:modele:shard': genere_modele_shard,
    'genere:doc': genere_doc,
    'liste:aiots': liste_aiots,
    'execute': execute_query,
    'build:index': build_primary_index
}

# ---- CLI ----
# The functions defined in this section are wrappers around the main Python
# API allowing them to be called directly from the terminal as a CLI
# executable/script.
def parse_args(args):
    """Parse command line parameters

    Args:
      args (List[str]): command line parameters as list of strings
          (for example  ``["--help"]``).

    Returns:
      :obj:`argparse.Namespace`: command line parameters namespace
    """
    parser = argparse.ArgumentParser(description="CLI pour la Boîte à Outils de l'inspection des Installations Classées (BOIC)")
    parser.add_argument(
        "--version",
        action="version",
        version=f"boic {__version__}",
    )


    parser.add_argument("-j", "--jewel", dest="root", help="La racine du dossier de l'inspection, par défaut la valeur est celle de la variable d'environnement JEWEL_PATH", type=pathlib.Path, metavar="JEWEL_PATH", default=_env_jewel_path)
    subparsers = parser.add_subparsers(dest="cmd", help='la commande à exécuter', required=True)

    parser_sync_aiot = subparsers.add_parser('sync:aiot', help='Synchronise l\'AIOT à partir des données GUN')

    parser_new_inspection = subparsers.add_parser('nouveau:inspection', help='Ajoute une nouvelle inspection')
    parser_new_inspection.add_argument('-d', '--depth', dest="max_depth", type=int, help="Profondeur maximal pour aller chercher les AIOTS.")

    parser_new_inspection = subparsers.add_parser('nouveau:aiot', help='Ajoute un nouvel aiot')
    parser_new_inspection.add_argument('-d', '--depth', dest="max_depth", type=int, help="Profondeur maximal pour aller chercher les AIOTS.")

    parser_build_index = subparsers.add_parser('build:index', help='Construit l\'index primaire des shards du jewel')
    parser_build_index.add_argument('-d', '--depth', dest="max_depth", type=int, help="Profondeur maximal pour indexer.")

    parser_execute = subparsers.add_parser('execute', help='Execute une requête SQL')
    parser_execute.add_argument('-d', '--depth', dest="max_depth", type=int, help="Profondeur maximal pour executer la requête.")

    parser_liste_aiots = subparsers.add_parser('liste:aiots', help='Liste les AIOTS')
    parser_liste_aiots.add_argument('-d', '--depth', dest="max_depth", type=int, help="Profondeur maximal pour rechercher les AIOTS")

    parser_genere_modele_shard = subparsers.add_parser('genere:modele:shard', help='Génère un shard à partir d\'un modèle')
    parser_genere_modele_shard.add_argument(dest="name", help="Nom du modèle")
    parser_genere_modele_shard.add_argument(dest="output", help="Chemin vers la destination du modèle")
    parser_genere_modele_shard.add_argument('-f', '--files', nargs='*', dest="context_files", help="Chemin vers des fichiers définissant des variables du modèle")

    parser_genere_modele_docx = subparsers.add_parser('genere:doc', help='Génère un document Word à partir d\'un modèle et d\'un shard (paramètre template)')
    parser_genere_modele_docx.add_argument(dest="shard", help="Lien vers le Shard")

    parser.add_argument(
        "-v",
        "--verbose",
        dest="loglevel",
        help="set loglevel to INFO",
        action="store_const",
        const=logging.INFO,
    )
    parser.add_argument(
        "-vv",
        "--very-verbose",
        dest="loglevel",
        help="set loglevel to DEBUG",
        action="store_const",
        const=logging.DEBUG,
    )
    return parser.parse_args(args)


def setup_logging(loglevel):
    """Setup basic logging

    Args:
      loglevel (int): minimum loglevel for emitting messages
    """
    logformat = "[%(asctime)s] %(levelname)s:%(name)s: %(message)s"
    logging.basicConfig(
        level=loglevel, stream=sys.stdout, format=logformat, datefmt="%Y-%m-%d %H:%M:%S"
    )

def main(args):
    """Wrapper allowing :func:`fib` to be called with string arguments in a CLI fashion

    Instead of returning the value from :func:`fib`, it prints the result to the
    ``stdout`` in a nicely formatted message.

    Args:
      args (List[str]): command line parameters as list of strings
          (for example  ``["--verbose", "42"]``).
    """
    args = parse_args(args)
    setup_logging(args.loglevel)

    jewel = J.open(args.root)
    _commands[args.cmd](jewel, args)


def run():
    """Calls :func:`main` passing the CLI arguments extracted from :obj:`sys.argv`

    This function can be used as entry point to create console scripts with setuptools.
    """
    main(sys.argv[1:])


if __name__ == "__main__":
    # ^  This is a guard statement that will prevent the following code from
    #    being executed in the case someone imports this file instead of
    #    executing it as a script.
    #    https://docs.python.org/3/library/__main__.html

    # After installing your project with pip, users can also run your Python
    # modules as scripts via the ``-m`` flag, as defined in PEP 338::
    #
    #     python -m boic.skeleton 42
    #
    run()

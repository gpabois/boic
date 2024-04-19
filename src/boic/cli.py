import argparse
import logging
import pathlib
import sys
import yaml
import os
from datetime import datetime

from boic import __version__, jewel as J, shards, templates, sql
from prettytable import PrettyTable

__author__ = "G. PABOIS"
__copyright__ = "G. PABOIS"
__license__ = "MIT"

_logger = logging.getLogger(__name__)
_env_jewel_path = pathlib.Path(os.environ['JEWEL_PATH']) if 'JEWEL_PATH' in os.environ else None

def read_context_files(jewel: J.Jewel, files):
    context = {}
    for file in files or []:
        file = jewel.root().join(file)
        if file.suffix in [".yaml", ".yml"]:
            context = {**context, **yaml.load(file.open(), Loader=yaml.Loader)}
        elif file.suffix in [".md"]:
            shard = shards.Shard.read(file)
            context = {**context, **shard.as_template_var()}
            
    return context

def read_query():
    lines = []
    line = input("Veuillez taper la requête SQL (Shard Query Language):\n").strip()

    while not line.endswith(';'):
        lines.append(line)
        line = input().strip()

    lines.append(line)
    query = " ".join(lines)
    return query

# --- COMMANDS HANDLERS ---
def new_inspection(jewel: J.Jewel, args):
    print("Créer une nouvelle inspection")
    nom = input("Nom de l'AIOT: ")

    if nom.startswith("jewel://"):
        candidate = shards.Shard.read(J.JewelPath.from_jewel_uri(jewel, nom))
    else:
        print(f"Recherche des candidats pour \"{nom}\"...")
        candidates = list(sql.execute(
            jewel, 
            f"SELECT * FROM aiot WHERE nom LIKE '%{nom}%'", 
            max_depth=args.max_depth
        ))

        if len(candidates) == 1:
            candidate = candidates[0]
        elif len(candidates) > 1:
            print("Les candidats suivants ont été trouvés : ")
            for i, candidate in enumerate(candidates, start=1):
                print(f"    {i}. {candidate['nom']}")
            i = input(f"Choisir entre {1}..{len(candidates)}: ") - 1
            candidate = candidates[i]
        else:
            print("Aucun candidat n'a été trouvé...")
            return 

    chemin_racine_aiot = candidate['path'].parent()
    print(f"Sélectionné: {candidate['nom']} ({chemin_racine_aiot})")

    nom = input("Nom de l'inspection: ")
    inspecteur_id = input("Inspecteur en charge (Abbrévation): ")
    inspecteur = shards.Shard.read(jewel.root().join("Equipe", f"{inspecteur_id}.md"))
    print(f"Inspecteur: {inspecteur['nom']} {inspecteur['prenom']}")

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
        equipe=inspecteur['equipe'],
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
   
    columns = []
    rows = []

    _logger.info("Execution de la requête...")
    for entry in sql.execute(jewel, query, max_depth=args.max_depth):
        for k in entry.keys():
            if k not in columns:
                columns.append(k)

        row = []
        for col in columns:
            if col not in entry:
                row.append("")
            else:
                row.append(str(entry[col]))
        
        rows.append(row)

    table = PrettyTable()
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
    shard = shards.Shard.read(shard_path)
    templates.new_docx_template(jewel, shard['template'], docx_path, **shard)

_commands = {
    'nouveau:inspection': new_inspection,
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

    parser_new_inspection = subparsers.add_parser('nouveau:inspection', help='Ajoute une nouvelle inspection')
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

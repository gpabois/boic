from typing import Optional
import os
import pathlib
import logging

from . import html
_logger = logging.getLogger(__name__)

class AssetManager:
    """ Gestionnaire d'assets de l'application """
    def __init__(self, dir: Optional[str] = None):
        self.dir = pathlib.Path(dir or os.getcwd(), 'assets')
    
    def __contains__(self, filepath: str) -> str:
        self.dir

class App:
    def __init__(self):
        _logger.info("Initialise la couche applicative")

    def process(self, ctx):
        """ Traite la requête, et écris une réponse. """
        _logger.info(f"Traitement de la requête: {ctx.uri()}")
        resp = ctx.response(status=200).set_header("Content-Type", "text/html").build()
        
        html.e("html", {}, [
            html.e("head", {}, [
                html.e("title", {}, ["BOIC"]),
                html.e("meta", {"charset": "UTF8"}),
                html.e("meta", {"name": "viewport", "content": "width=device-width, initial-scale=1.0"}),
                html.e("script", {"src": "https://cdn.tailwindcss.com"}, [""])
            ]),
            html.e("body", {"class": "bg-slate-600"}, [
                html.e("h1", {}, ["Hello world"])
            ])
        ]).write_to_stream(resp)

        resp.close()

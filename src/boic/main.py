from __future__ import annotations
from typing import Optional
import logging
import sys
import platform
import argparse
import io

from boic import __version__
from boic.app import App

from cefpython3 import cefpython as cef

_logger = logging.getLogger(__name__)

def parse_args(args):
    """Parse command line parameters

    Args:
      args (List[str]): command line parameters as list of strings
          (for example  ``["--help"]``).

    Returns:
      :obj:`argparse.Namespace`: command line parameters namespace
    """
    parser = argparse.ArgumentParser(description="Application pour la Boîte à Outils de l'inspection des Installations Classées (BOIC)")
    
    parser.add_argument(
        "--version",
        action="version",
        version=f"boic {__version__}",
    )

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
        level=loglevel, stream=sys.stdout, format=logformat, datefmt="%d/%m/%Y %H:%M:%S"
    )

class ClientHandler:
    """ Gestionnaire du client (interface avec le Browser) """
    
    def __init__(self, app: App):
        self.app = app
        self.res_refs = []
        
    def GetResourceHandler(self, browser, frame, request) -> Optional[cef.ResourceHandler]:
        """ 
            Retourne le gestionnaire de resources.
        """
        
        uri = request.GetUrl()
        
        _logger.info(f"Requête demandée : {uri}")
        
        if uri.startswith("https://app"):
            appResourceHandler = AppResourceHandler(app=self.app)
            self.res_refs.append(appResourceHandler)
            return appResourceHandler

        return None

class AppRequestContext:
    """ Représente le contexte d'exécution d'une requête """
    def __init__(self, resource_handler: AppResourceHandler):
        self.resource_handler = resource_handler
    
    def uri(self) -> str:
        return self.resource_handler.request.GetUrl().removeprefix("https://app")

    def response(self, status) -> AppResponseBuilder:
        return AppResponseBuilder(resource_handler=self.resource_handler, status=status)

class AppResponseBuilder:
    def __init__(self, resource_handler: AppResourceHandler, status: int = 200):
        self.resource_handler = resource_handler
        self.status = status
        self.status_text = "Unknown"
        self.headers = {}
    
    def set_header(self, key: str, value: any) -> AppResponseBuilder:
        self.headers[key] = value
        return self

    def build(self) -> AppResponse:
        resp = AppResponse(resource_handler=self.resource_handler, status=self.status, headers=self.headers)
        self.resource_handler.response = resp
        return resp

class AppResponse:
    """ Représente une réponse applicative """
    def __init__(self, resource_handler: AppResourceHandler, status: int, headers):
        self.stream = io.BytesIO()
        self.closed = False
        
        self.status = status
        self.status_text = "Unknown"

        self.headers = headers

        self.resource_handler = resource_handler

        self._written = 0
        self._read = 0
    
    def write(self, b: bytes) -> int:
        self.stream.seek(self._written)
        written = self.stream.write(b)
        self._written += written
        return written

    def read(self, size: int =-1) -> bytes:
        self.stream.seek(self._read)
        chunk = self.stream.read(size)
        self._read += len(chunk)
        return chunk
    
    def flush(self):
        self.resource_handler.resume()

    def close(self):
        """ Ferme le flux de réponse """
        self.closed = True
        self.flush()

class AppResourceHandler:
    """ Gère la resource applicative """
    def __init__(self, app: App):
        _logger.info("Requête applicative détectée")
        self.app = app

    def resume(self):
        """ Poursuit la gestion de la ressource """
        self.callback.Continue()

    def ProcessRequest(self, request, callback) -> bool:
        """
            Begin processing the request. 

            To handle the request return True 
            and call Callback.Continue() once the response header information is available 
            (Callback::Continue() can also be called from inside this method if header information is available immediately). 

            To cancel the request return False.
        """

        _logger.info(f"Démarre le traitement de la requête applicative {request.GetUrl()}")

        self.request = request
        self.callback = callback

        self.app.process(
            AppRequestContext(resource_handler=self)
        )

        return True

    def GetResponseHeaders(self, response: cef.Response, responseLengthOut: list[int], redirectUrlOut: list[str]):
        """
            Retrieve response header information. 
            
            If the response length is not known set |response_length_out[0]| to -1 and ReadResponse() will be called until it returns false. 
            If the response length is known set |response_length_out[0]| to a positive value 
            and ReadResponse() will be called until it returns false or the specified number of bytes have been read. 
            
            Use the |response| object to set the mime type, http status code and other optional header values. 
            
            To redirect the request to a new URL set |redirect_url_out[0]| to the new URL. 
            
            If an error occured while setting up the request you can call SetError() on |response| to indicate the error condition.
        """
        assert self.response, "Aucune réponse reçue"

        resp = self.response
        
        response.SetStatus(resp.status)
        response.SetStatusText(resp.status_text)
        
        if resp.headers:
            response.SetHeaderMap(resp.headers)

        if 'Content-Type' in resp.headers:
            response.SetMimeType(resp.headers['Content-Type'])
        else:
            response.SetMimeType("text/plain")
  
        if "Content-Length" in resp.headers:
            responseLengthOut[0] = resp.headers['Content-Length']
        else:
            responseLengthOut[0] = -1

    def ReadResponse(self, data_out: list[bytes], bytes_to_read: int, bytes_read_out: list[int], callback: cef.Callback):
        """
            Read response data. 

            If data is available immediately copy up to |bytes_to_read| bytes into |data_out|, set |bytes_read_out| 
            to the number of bytes copied, and return true. 
            
            To read the data at a later time set |bytes_read_out| to 0, 
            return true and call callback.Continue() when the data is available. 
            
            To indicate response completion return false.
        """       
        # Redonne le nouvelle callback pour la suite de l'écriture dans le flux.
        self.callback = callback

        # On a de la donnée à envoyer dans le flux. 
        chunk = self.response.read(size=bytes_to_read)

        data_out[0] = chunk
        bytes_read_out[0] = len(chunk)

        if chunk:
            return True

        # Le flux de la réponse est fermée
        return not self.response.closed

    def CanGetCookie(self, cookie):
        # Return true if the specified cookie can be sent
        # with the request or false otherwise. If false
        # is returned for any cookie then no cookies will
        # be sent with the request.
        return True

    def CanSetCookie(self, cookie):
        # Return true if the specified cookie returned
        # with the response can be set or false otherwise.
        return True

    def Cancel(self):
        # Request processing has been canceled.
        pass

class AppRequestHandler:
    """ Gère la vie de la requête vers l'application """
    def __init__(self, app: App, resourceHandler: AppResourceHandler):
        self.app = app
        self.resourceHandler = resourceHandler
        self.data = []

    def OnUploadProgress(self, webRequest, current, total):
        pass

    def OnDownloadProgress(self, webRequest, current, total):
        pass

    def OnDownloadData(self, webRequest, data):
        self.data += data

    def OnRequestComplete(self, webRequest):
        # cefpython.WebRequest.Status = {"Unknown", "Success",
        # "Pending", "Canceled", "Failed"}
        statusText = "Unknown"
        
        if webRequest.GetRequestStatus() in cef.WebRequest.Status:
            statusText = cefpython.WebRequest.Status[webRequest.GetRequestStatus()]
        
        self.response = webRequest.GetResponse()

        self.data = "hello world" #self.process(request=webRequest.GetRequest())
        self.dataLength = len(self.data)

        self.resourceHandler.callback.Continue()

def init(args):
    sys.excepthook = cef.ExceptHook

    app = App()

    _logger.info("Création de l'interface client avec CEF")
    client_handler = ClientHandler(app=app)

    _logger.info("Initialise CEF")
    cef.Initialize()

    _logger.info("Création de l'instance de navigation")
    browser = cef.CreateBrowserSync(url="http://app", window_title=f"BOIC {__version__}")
    browser.SetClientHandler(client_handler)

    _logger.info("Démarrage de la boucle évènementielle")
    cef.MessageLoop()

    del browser

    _logger.info("Arrête CEF")
    cef.Shutdown()

def main(args):
    args = parse_args(args)
    setup_logging(args.loglevel)
    check_versions()
    init(args)

def check_versions():
    ver = cef.GetVersion()
    _logger.info("CEF Python {ver}".format(ver=ver["version"]))
    _logger.info("Chromium {ver}".format(ver=ver["chrome_version"]))
    _logger.info("CEF {ver}".format(ver=ver["cef_version"]))
    _logger.info("Python {ver} {arch}".format(
           ver=platform.python_version(),
           arch=platform.architecture()[0]))

    _logger.info("BOIC {ver}".format(ver=__version__))
    assert cef.__version__ >= "57.0", "CEF Python v57.0+ required to run this"

def run():
    """Calls :func:`main` passing the CLI arguments extracted from :obj:`sys.argv`

    This function can be used as entry point to create console scripts with setuptools.
    """
    main(sys.argv[1:])
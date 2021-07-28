from index.identifier.identifiermanager import IdentifierManager
from re import sub, match
from urllib.parse import unquote, quote
from requests import get
from json import loads
from index.storer.csvmanager import CSVManager
from requests import ReadTimeout
from requests.exceptions import ConnectionError
from time import sleep
from bs4 import BeautifulSoup
from tempfile import NamedTemporaryFile
import shutil
import csv


class PMIDManager( IdentifierManager ):
    def __init__(self, valid_pmid=None, use_api_service=True):
        if valid_pmid is None:
            valid_pmid = CSVManager( store_new=False )

        self.api = "https://pubmed.ncbi.nlm.nih.gov/"
        self.valid_pmid = valid_pmid
        self.use_api_service = use_api_service
        self.p = "pmid:"
        super( PMIDManager, self ).__init__()

    def set_valid(self, id_string):
        pmid = self.normalise( id_string, include_prefix=True )

        if self.valid_pmid.get_value( pmid ) is None:
            self.valid_pmid.add_value( pmid, "v" )

    def is_valid(self, id_string):
        pmid = self.normalise( id_string, include_prefix=True )
        if pmid is None or match( "^pmid:[1-9]\d*$", pmid ) is None:
            return False
        else:
            if self.valid_pmid.get_value( pmid ) is None:
                if self.__pmid_exists( pmid ):
                    self.valid_pmid.add_value( pmid, "v" )
                else:
                    self.valid_pmid.add_value( pmid, "i" )
            return "v" in self.valid_pmid.get_value( pmid )

    def normalise(self, id_string, include_prefix=False):
        try:
            pmid_string = sub( "^0+", "", sub( "\0+", "", (sub( "[^\d+]", "", id_string )) ) )
            return "%s%s" % (self.p if include_prefix else "", pmid_string)
        except:
            return None

    def __pmid_exists(self, pmid_full):
        pmid = self.normalise( pmid_full )
        if self.use_api_service:
            print( "usa API?" )
            tentative = 3
            while tentative:
                tentative -= 1
                try:
                    print( self.api + quote( pmid ) + "/?format=pmid" )
                    r = get( self.api + quote( pmid ) + "/?format=pmid", headers=self.headers, timeout=30 )
                    if r.status_code == 200:
                        r.encoding = "utf-8"
                        print("Did I find the ID?", r.text, pmid)

                        soup = BeautifulSoup( r.content )
                        print( "This is BS soup", soup )
                        for i in soup.find_all( "meta", {"name": "uid"} ):
                            id = i["content"]
                            print("The id is: ", id)
                            print("The pm id is: ", pmid)
                            if id == pmid:
                                print("id == pmid: returns true")
                                return True

                except ReadTimeout:
                    pass
                except ConnectionError:
                    sleep(5)

        return False

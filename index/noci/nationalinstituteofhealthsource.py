from os import walk, sep, remove
from os.path import isdir
from json import load
from csv import DictWriter
from index.citation.citationsource import CSVFileCitationSource
from index.identifier.pmidmanager import PMIDManager
from index.citation.oci import Citation, OCIManager


class NIHCitationSource( CSVFileCitationSource ):
    def __init__(self, src, local_name=""):
        self.pmid = PMIDManager()
        super( NIHCitationSource, self ).__init__( src, local_name )

    def get_next_citation_data(self):
        row = self._get_next_in_file()
        #id_type = OCIManager.pmid_type

        while row is not None:
            citing = self.pmid.normalise(row.get("citing"))
            cited = self.pmid.normalise(row.get("referenced"))

            self.update_status_file()
            return citing, cited, None, None, None, None #, id_type
            self.update_status_file()
            row = self._get_next_in_file()

        remove(self.status_file)


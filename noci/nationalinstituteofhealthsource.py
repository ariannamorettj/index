from os import walk, sep, remove
from os.path import isdir
from json import load
from csv import DictWriter
from index.citation.citationsource import CSVFileCitationSource
from index.identifier.pmidmanager import PMIDManager
from index.citation.oci import Citation, OCIManager


class NationalInstituteHealthSource(CSVFileCitationSource):
    def __init__(self, src, local_name=""):
        self.pmid = PMIDManager()
        super(NationalInstituteHealthSource, self).__init__(src, local_name) #, id_type=OCIManager.pmid_type

    def get_next_citation_data(self):
        row = self._get_next_in_file()
        id_type = OCIManager.pmid_type

        while row is not None:
            citing = self.pmid.normalise(row.get("citing"))
            cited = self.pmid.normalise(row.get("referenced"))

            if citing is not None and cited is not None:
                created = row.get("citing_publication_date")
                if not created:
                    created = None

                cited_pub_date = row.get("cited_publication_date")
                if not cited_pub_date:
                    timespan = None

                else:
                    c = Citation(None, None, created, None, cited_pub_date, None, None, None, None, "", None, None, None, None, None)
                    timespan = c.duration

                self.update_status_file()
                return citing, cited, created, timespan, None, None #, id_type
            self.update_status_file()
            row = self._get_next_in_file()

        remove(self.status_file)
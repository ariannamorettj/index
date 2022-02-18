import unittest
from index.coci.glob import process
from os import sep, makedirs
from os.path import exists
from shutil import rmtree
from index.storer.csvmanager import CSVManager
from index.noci.nationalinstituteofhealthsource import NIHCitationSource
from csv import DictReader


class NOCITest(unittest.TestCase):

    def setUp(self):
        self.input_file = "index%stest_data%snih_dump%ssource.csv" % (sep, sep, sep)
        self.citations = "index%stest_data%snih_dump%scitations.csv" % (sep, sep, sep)

    def test_citation_source(self):
        ns = NIHCitationSource( self.input_file )
        new = []
        cit = ns.get_next_citation_data()
        while cit is not None:
            citing, cited, creation, timespan, journal_sc, author_sc = cit
            new.append({
                "citing": citing,
                "cited": cited,
                "creation": "" if creation is None else creation,
                "timespan": "" if timespan is None else timespan,
                "journal_sc": "no" if journal_sc is None else journal_sc,
                "author_sc": "no" if author_sc is None else author_sc
            })
            cit = ns.get_next_citation_data()

        with open(self.citations, encoding="utf8") as f:
            old = list(DictReader(f))

        self.assertEqual(new, old)
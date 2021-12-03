import unittest
from os import sep, remove, scandir, rmdir, makedirs
import os
import shutil
import re
from os.path import exists
from index.noci.glob1 import issn_data_recover, issn_data_to_cache, build_pubdate, get_all_files, process
from index.storer.csvmanager import CSVManager
from index.identifier.issnmanager import ISSNManager
from index.identifier.orcidmanager import ORCIDManager
from index.identifier.pmidmanager import PMIDManager
from index.identifier.doimanager import DOIManager
import shutil
from rdflib import Graph
import csv
import pandas as pd

class MyTestCase( unittest.TestCase ):
    def setUp(self):
        self.dir_with_issn_map = "index%stest_data%sglob_noci%sissn_data_recover%swith_issn_mapping" % (sep, sep, sep, sep)
        self.dir_without_issn_map = "index%stest_data%sglob_noci%sissn_data_recover%swithout_issn_mapping" % (sep, sep, sep, sep)
        self.issn_journal_sample_dict = {"N Biotechnol": ["1871-6784"], "Biochem Med": ["0006-2944"], "Magn Reson Chem": ["0749-1581"]}
        self.data_to_cache_dir = "index%stest_data%sglob_noci%sissn_data_to_cache" % (sep, sep, sep)
        self.get_all_files_dir = "index%stest_data%sglob_noci%sget_all_files" % (sep, sep, sep)
        self.csv_sample = "index%stest_data%sglob_noci%sget_all_files%s1.csv" % (sep, sep, sep, sep)
        self.output_dir = "index%stest_data%sglob_noci%sprocess%soutput" % (sep, sep, sep, sep)
        self.valid_pmid = CSVManager( self.output_dir + sep + "valid_pmid.csv" )
        self.valid_doi = CSVManager( "index/test_data/crossref_glob" + sep + "valid_doi.csv" )
        self.id_date = CSVManager( self.output_dir + sep + "id_date_pmid.csv" )
        self.id_issn = CSVManager( self.output_dir + sep + "id_issn_pmid.csv" )
        self.id_orcid = CSVManager( self.output_dir + sep + "id_orcid_pmid.csv" )
        self.doi_manager = DOIManager(self.valid_doi)
        self.pmid_manager = PMIDManager(self.valid_pmid)
        self.issn_manager = ISSNManager()
        self.orcid_manager = ORCIDManager()
        self.sample_reference = "pmid:7829625"

    def test_issn_data_recover(self):
        #Test the case in which there is no mapping file for journals - issn
        self.assertEqual(issn_data_recover(self.dir_without_issn_map), {})
        #Test the case in which there is a mapping file for journals - issn
        issn_map_dict_len = len(issn_data_recover(self.dir_with_issn_map))
        self.assertTrue(issn_map_dict_len>0)

    def test_issn_data_to_cache(self):
        filename = self.data_to_cache_dir + sep + 'journal_issn.json'
        if exists(filename):
            remove(filename)
        self.assertFalse(exists(filename))
        issn_data_to_cache(self.issn_journal_sample_dict, self.data_to_cache_dir)
        self.assertTrue(exists(filename))

    def test_get_all_files(self):
        all_files, opener = get_all_files( self.get_all_files_dir)
        len_all_files = len(all_files)
        #The folder contains 4 csv files, but two of those contains the words "citations" or "source" in their filenames
        self.assertEqual( len_all_files, 2)

    def test_build_pubdate(self):
        df = pd.DataFrame()
        for chunk in pd.read_csv(self.csv_sample, chunksize=1000):
            f = pd.concat( [df, chunk], ignore_index=True )
            f.fillna( "", inplace=True )
            for index, row in f.iterrows():
                pub_date = build_pubdate(row)
                self.assertTrue(isinstance(pub_date, str))
                self.assertTrue(isinstance(int(pub_date), int))
                self.assertEqual(len(pub_date), 4)

    def test_process(self):
        for files in os.listdir( self.output_dir):
            path = os.path.join( self.output_dir, files )
            try:
                shutil.rmtree(path)
            except OSError:
                os.remove(path)
        self.assertEqual(len(os.listdir(self.output_dir)),0)
        process(self.get_all_files_dir, self.output_dir, 20)
        self.assertTrue(len(os.listdir(self.output_dir))>0)

        df = pd.DataFrame()
        for chunk in pd.read_csv( self.csv_sample, chunksize=1000 ):
            f = pd.concat( [df, chunk], ignore_index=True )
            f.fillna( "", inplace=True )
            for index, row in f.iterrows():
                if index == 1:
                    pmid = row["pmid"]
                    doi = row["doi"]

        citing_pmid = "pmid:" + self.pmid_manager.normalise(pmid)
        #corresp_doi = self.doi_manager.normalise(doi)

        self.assertEqual(self.valid_pmid.get_value(citing_pmid), {'v'})
        self.assertEqual(self.valid_pmid.get_value(self.sample_reference), {'v'})
        self.assertEqual(self.id_date.get_value(citing_pmid), {'1998'})
        self.assertEqual(self.id_issn.get_value(citing_pmid), {'0918-8959', '1348-4540'})


if __name__ == '__main__':
    unittest.main()

#python -m unittest index.test.13_glob
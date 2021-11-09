import unittest
from os import sep, remove
from os.path import exists
from index.mapping_global.mapping import get_all_files, process, create_rdf_from_csv
from index.storer.csvmanager import CSVManager
from index.identifier.pmidmanager import PMIDManager
from index.identifier.doimanager import DOIManager
import pandas as pd
import shutil

class MappingTest( unittest.TestCase ):
    def setUp(self):
        self.input_dir_cnc_csv = "index%snoci_test%sdata%scsv" % (sep, sep, sep)
        self.input_dir_mappings = "index%stest_data%sextrasamp_nih_mapping_global%sextrasamp_NIH_mapping" % (sep, sep, sep)
        self.output_dir = "index%stest_data%smapping_test_output" % (sep, sep)
        self.metaid_mappingfile = "index%stest_data%smapping_global%smetaidmapping.csv" % (sep, sep, sep)
        self.canc_to_new_mappingfile = "index%stest_data%smapping_global%scanc_to_new_mid.csv" % (sep, sep, sep)
        self.last_metaid_file = "index%stest_data%smapping_global%slast_metaid.txt" % (sep, sep, sep)
        self.pmid_manager = PMIDManager()
        self.doi_manager = DOIManager()

    def test_get_all_files(self):
        all_files, opener = get_all_files(self.input_dir_cnc_csv)
        len_all_files = len(all_files)
        self.assertEqual(len_all_files, 1)

        all_map_files, opener = get_all_files(self.input_dir_mappings)
        len_all_map_files = len(all_map_files)
        self.assertEqual(len_all_map_files, 6)

    def test_proess_EntryCreation(self):
        if exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        self.assertFalse(exists(self.output_dir))

        if exists(self.metaid_mappingfile):
            remove(self.metaid_mappingfile)
        self.assertFalse(exists(self.metaid_mappingfile))

        if exists(self.canc_to_new_mappingfile):
            remove(self.canc_to_new_mappingfile)
        self.assertFalse(exists(self.canc_to_new_mappingfile))

        if exists(self.last_metaid_file):
            remove(self.last_metaid_file)
        self.assertFalse(exists(self.last_metaid_file))

        process(self.input_dir_cnc_csv, self.input_dir_mappings,self.metaid_mappingfile, self.canc_to_new_mappingfile,self.last_metaid_file, self.output_dir)
        self.assertTrue(exists(self.output_dir))
        self.assertTrue(exists(self.metaid_mappingfile))
        self.assertTrue(exists(self.canc_to_new_mappingfile))
        self.assertTrue(exists(self.output_dir))

if __name__ == '__main__':
    unittest.main()
#C:\Users\arimoretti\Documents\GitHub\index>python -m unittest index.test.12_mapping
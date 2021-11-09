import unittest
from os import sep
from os.path import exists
from index.noci.nocimapping import get_all_files, process
from index.storer.csvmanager import CSVManager
from index.identifier.pmidmanager import PMIDManager
from index.identifier.doimanager import DOIManager
import pandas as pd
import shutil

class NociMappingTest( unittest.TestCase ):

    def setUp(self):
        self.input_dir = "index%stest_data%snih_dump" % (sep, sep)
        self.output_dir = "index%stest_data%snih_mapping_global" % (sep, sep)
        self.pmid_manager = PMIDManager()
        self.doi_manager = DOIManager()

    def test_get_all_files(self):
        all_files, opener = get_all_files(self.input_dir)
        len_all_files = len(all_files)
        self.assertEqual(len_all_files, 2)

    def test_process(self):
        mapping_folder = self.output_dir + "/NIH_mapping"
        if exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        self.assertFalse(exists(self.output_dir))
        process(self.input_dir, self.output_dir)
        self.assertTrue(exists(self.output_dir))
        self.assertTrue(exists(mapping_folder))
        all_files, opener = get_all_files(self.input_dir)
        sample_file = all_files[0]
        df = pd.DataFrame()
        for chunk in pd.read_csv(sample_file, chunksize=1000):
            f = pd.concat( [df, chunk], ignore_index=True )
            for index, row in f.iterrows():
                if index == 1:
                    pmid = self.pmid_manager.p + str(row["pmid"])
                    doi = self.doi_manager.p + str(row["doi"])
        all_files_output, opener = get_all_files(mapping_folder)
        for file_idx, file in enumerate(all_files_output, 1):
            csv_man_file = CSVManager(file)
            if csv_man_file.get_value(pmid) is not None:
                self.assertEqual(csv_man_file.get_value(pmid),{doi})

if __name__ == '__main__':
    unittest.main()

#C:\Users\arimoretti\Documents\GitHub\index>python -m unittest index.test.11_nocimapping
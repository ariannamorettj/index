import unittest
from os import sep, remove, scandir
import re
from os.path import exists
from index.mapping_global.mapping import get_all_files, process, create_rdf_from_csv
from index.storer.csvmanager import CSVManager
from index.identifier.pmidmanager import PMIDManager
from index.identifier.doimanager import DOIManager
import shutil
from rdflib import Graph


class MappingTest( unittest.TestCase ):
    def setUp(self):
        self.input_dir_cnc_csv = "index%snoci_test%sdata%scsv" % (sep, sep, sep)
        self.input_dir_cnc_csv_extra = "index%stest_data%sextrasamp_nih_mapping_global%sextrasamp_cnc_output" % (sep, sep, sep)
        self.input_dir_mappings = "index%stest_data%snih_mapping_global%sNIH_mapping" % (sep, sep, sep)
        self.input_dir_mappings_extra = "index%stest_data%sextrasamp_nih_mapping_global%sextrasamp_NIH_mapping" % (sep, sep, sep)
        self.output_dir = "index%stest_data%smapping_test_output" % (sep, sep)
        self.metaid_mappingfile = "index%stest_data%smapping_global%smetaidmapping.csv" % (sep, sep, sep)
        self.canc_to_new_mappingfile = "index%stest_data%smapping_global%scanc_to_new_mid.csv" % (sep, sep, sep)
        self.last_metaid_file = "index%stest_data%smapping_global%slast_metaid.txt" % (sep, sep, sep)
        self.pmid_manager = PMIDManager()
        self.doi_manager = DOIManager()
        self.input_csv_add = "index%stest_data%smapping_global%srdf_from_csv%scsvadd.csv" % (sep, sep, sep, sep)
        self.input_csv_del = "index%stest_data%smapping_global%srdf_from_csv%scsvdel.csv" % (sep, sep, sep, sep)
        self.rdf_from_csv_dir = "index%stest_data%smapping_global%srdf_from_csv" % (sep, sep, sep)
        self.excluded_metaid = "https://w3id.org/oc/meta/br/0605"

    def test_get_all_files(self):
        all_files, opener = get_all_files(self.input_dir_cnc_csv)
        len_all_files = len(all_files)
        self.assertEqual(len_all_files, 1)

        all_map_files_extra, opener = get_all_files(self.input_dir_mappings_extra)
        len_all_map_files_extra = len(all_map_files_extra)
        self.assertEqual(len_all_map_files_extra, 6)

        all_map_files, opener = get_all_files(self.input_dir_mappings)
        len_all_map_files = len(all_map_files)
        self.assertEqual(len_all_map_files, 1)

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
        self.assertTrue(exists(self.last_metaid_file))

    def test_process_metaid_reassignment(self):
        csvman_metaidmapping = CSVManager(self.metaid_mappingfile)
        metaid_pre_mapping_1 = csvman_metaidmapping.get_value("pmid:2140506")
        metaid_pre_mapping_2 = csvman_metaidmapping.get_value("pmid:1509982")

        process(self.input_dir_cnc_csv, self.input_dir_mappings_extra, self.metaid_mappingfile,
                 self.canc_to_new_mappingfile, self.last_metaid_file, self.output_dir )

        csvman_metaidmapping2 = CSVManager(self.metaid_mappingfile)
        metaid_post_mapping_1 = csvman_metaidmapping2.get_value("pmid:2140506")
        metaid_post_mapping_2 = csvman_metaidmapping2.get_value("pmid:1509982")

        #check that after the addition of new mapping files stating that two ids point to the same object, the one with
        #the lowest metaid maintains it, while the other is remapped to the metaid with the lowest numerical value.
        self.assertEqual(metaid_pre_mapping_1, metaid_post_mapping_1)
        self.assertEqual(metaid_pre_mapping_1, metaid_post_mapping_2)
        self.assertEqual(metaid_post_mapping_1, metaid_post_mapping_2)
        self.assertNotEqual(metaid_pre_mapping_2, metaid_post_mapping_2)

        #check that all the ids which point to the same object are associated with the same metaid
        self.assertEqual(metaid_pre_mapping_1, csvman_metaidmapping2.get_value("pmid:2140506"))
        self.assertEqual(metaid_pre_mapping_1, csvman_metaidmapping2.get_value("pmid:1509982"))
        self.assertEqual(metaid_pre_mapping_1, csvman_metaidmapping2.get_value("pmid:1968312"))
        self.assertEqual(metaid_pre_mapping_1, csvman_metaidmapping2.get_value("pmid:8330864"))
        self.assertEqual(metaid_pre_mapping_1, csvman_metaidmapping2.get_value("pmid:443799111"))
        self.assertEqual(metaid_pre_mapping_1, csvman_metaidmapping2.get_value("doi:10.1002/mrc.2517"))

        #check that, in case during the same run an id is assigned a metaid which is then invalidated, and so it is
        #assigned another metaid, only the information about the final metaid is registered.
        #check that canc_to_new_mid csv file keeps track of the metaid-to-metaid remappings
        csvman_canctonew = CSVManager(self.canc_to_new_mappingfile)
        prev_metaid = list(metaid_pre_mapping_2)[0]
        print("csvman_canctonew.get_value(prev_metaid)", csvman_canctonew.get_value(prev_metaid))
        self.assertEqual(csvman_canctonew.get_value(prev_metaid), metaid_pre_mapping_1)


    def test_last_metaid_update(self):
        with open(self.last_metaid_file, "r" ) as l_mid:
            data = l_mid.read()
            match = re.search( '(\d+)', data )
            if match:
                last_metaid_prev = int( match.group(0))

        process(self.input_dir_cnc_csv_extra, self.input_dir_mappings, self.metaid_mappingfile,
                 self.canc_to_new_mappingfile, self.last_metaid_file, self.output_dir)

        g = Graph()
        for entry in scandir(self.output_dir):
            if "new_mappings" in entry.path and entry.is_file():
                new_triples_nt = entry

        g.parse(new_triples_nt.path, format="nt" )
        new_ids_mapped = 0
        for index, (s, p, o) in enumerate(g):
            new_ids_mapped += 1

        with open(self.last_metaid_file, "r" ) as l_mid:
            data = l_mid.read()
            match = re.search( '(\d+)', data )
            if match:
                last_metaid_cur = int( match.group(0))

        #check that the last metaid number has grown of as many unities as the number of the new mapped ids
        self.assertEqual(last_metaid_cur, (last_metaid_prev + new_ids_mapped))


    def test_create_rdf_from_csv(self):
        input_dir = self.rdf_from_csv_dir
        num_input_files = 0
        for entry in scandir(input_dir):
            if entry.path.endswith(".nt") and entry.is_file():
                remove(entry)
                self.assertFalse(exists(entry))
            elif entry.path.endswith(".csv") and entry.is_file():
                num_input_files += 1

        create_rdf_from_csv(self.input_csv_add, self.input_csv_del)
        num_nt_files = 0
        for entry in scandir(input_dir):
            if entry.path.endswith(".nt") and entry.is_file():
                num_nt_files += 1
                g = Graph()
                g.parse(entry.path, format="nt")

                #check that the triples with the same metaid object present in both the
                # additions file and in the deletions file were not added at all.
                for s, p, o in g:
                    self.assertTrue(o != self.excluded_metaid)

        #check that a nt file was created for each csv input file
        self.assertTrue(num_input_files == num_nt_files)


if __name__ == '__main__':
    unittest.main()
#C:\Users\arimoretti\Documents\GitHub\index>python -m unittest index.test.12_mapping
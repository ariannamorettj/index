import unittest
from os import sep, remove, scandir, rmdir, makedirs
import os
import shutil
import re
from os.path import exists
from index.mapping_global.mapping import get_all_files, process, create_rdf_from_csv
from index.storer.csvmanager import CSVManager
from index.identifier.pmidmanager import PMIDManager
from index.identifier.doimanager import DOIManager
import shutil
from rdflib import Graph
import csv


class MappingTest( unittest.TestCase ):
    def setUp(self):
        self.input_dir_cnc_csv = "index%snoci_test%sdata%scsv" % (sep, sep, sep)
        self.input_dir_cnc_csv_extra = "index%stest_data%sextrasamp_nih_mapping_global%sextrasamp_cnc_output" % (sep, sep, sep)
        self.input_dir_mappings = "index%stest_data%snih_mapping_global%sNIH_mapping" % (sep, sep, sep)
        self.input_dir_mappings_extra = "index%stest_data%sextrasamp_nih_mapping_global%sextrasamp_NIH_mapping" % (sep, sep, sep)

        #copies of the files for the test: test_proess_EntryCreation(self)
        self.output_dir = "index%stest_data%smapping_test_output" % (sep, sep)
        self.metaid_mappingfile = "index%stest_data%smapping_global%smetaidmapping.csv" % (sep, sep, sep)
        self.canc_to_new_mappingfile = "index%stest_data%smapping_global%scanc_to_new_mid.csv" % (sep, sep, sep)
        self.last_metaid_file = "index%stest_data%smapping_global%slast_metaid.txt" % (sep, sep, sep)

        #copies of the files for the test: test_process_metaid_reassignment(self)
        self.output_dir_1 = "index%stest_data%smapping_test_output_1" % (sep, sep)
        self.metaid_mappingfile_1 = "index%stest_data%smapping_global%smetaidmapping_1.csv" % (sep, sep, sep)
        self.canc_to_new_mappingfile_1 = "index%stest_data%smapping_global%scanc_to_new_mid_1.csv" % (sep, sep, sep)
        self.last_metaid_file_1 = "index%stest_data%smapping_global%slast_metaid_1.txt" % (sep, sep, sep)

        #copies of the files for the test: test_last_metaid_update(self)
        self.output_dir_2 = "index%stest_data%smapping_test_output_2" % (sep, sep)
        self.metaid_mappingfile_2 = "index%stest_data%smapping_global%smetaidmapping_2.csv" % (sep, sep, sep)
        self.canc_to_new_mappingfile_2 = "index%stest_data%smapping_global%scanc_to_new_mid_2.csv" % (sep, sep, sep)
        self.last_metaid_file_2 = "index%stest_data%smapping_global%slast_metaid_2.txt" % (sep, sep, sep)

        self.pmid_manager = PMIDManager()
        self.doi_manager = DOIManager()

        self.input_csv_add = "index%stest_data%smapping_global%srdf_from_csv%scsvadd.csv" % (sep, sep, sep, sep)
        self.input_csv_del = "index%stest_data%smapping_global%srdf_from_csv%scsvdel.csv" % (sep, sep, sep, sep)
        self.rdf_from_csv_output_dir = "index%stest_data%smapping_global%srdf_from_csv_out" % (sep, sep, sep)

        self.excluded_metaid = "https://w3id.org/oc/meta/br/0605"
        self.excluded_metaid_value = "5"

    def test_get_all_files(self):
        print("1# TEST")
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
        print("2# TEST")
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
        print("3# TEST")

        if exists(self.output_dir_1):
            shutil.rmtree(self.output_dir_1)

        if exists(self.metaid_mappingfile_1):
            remove(self.metaid_mappingfile_1)
            header = ["id", "value"]
            with open(self.metaid_mappingfile_1, 'w', newline="" ) as midmcsv_file:
                writer = csv.writer( midmcsv_file )
                writer.writerow( header )

        if exists(self.canc_to_new_mappingfile_1):
            remove( self.canc_to_new_mappingfile_1)
            header = ["id", "value"]
            with open(self.canc_to_new_mappingfile_1, 'w', newline="" ) as canc_to_new_csv_file:
                writer = csv.writer(canc_to_new_csv_file)
                writer.writerow( header )

        if exists(self.last_metaid_file_1):
            remove(self.last_metaid_file_1)
        with open(self.last_metaid_file_1, 'w' ) as l_mid:
            l_mid.write( "0" )

        with open(self.last_metaid_file_1, "r" ) as l_mid:
            data = l_mid.read()
            match = re.search( '(\d+)', data )
            if match:
                next_metaid = int( match.group( 0 ) ) + 1
                print( "next_metaid", next_metaid )

        process(self.input_dir_cnc_csv, self.input_dir_mappings, self.metaid_mappingfile_1, self.canc_to_new_mappingfile_1,
                 self.last_metaid_file_1, self.output_dir_1)

        csvman_metaidmapping = CSVManager(self.metaid_mappingfile_1)

        print( "BEFORE taking into account new mapping files" )
        with open(self.metaid_mappingfile_1) as f:
            reader = csv.reader( f )
            for row in reader:
                print( " ".join( row ) )

        metaid_pre_mapping_1 = csvman_metaidmapping.get_value("pmid:2140506")
        metaid_pre_mapping_2 = csvman_metaidmapping.get_value("pmid:1509982")

        process(self.input_dir_cnc_csv, self.input_dir_mappings_extra, self.metaid_mappingfile_1,
                 self.canc_to_new_mappingfile_1, self.last_metaid_file_1, self.output_dir_1 )

        print( "AFTER taking into account new mapping files" )
        with open(self.metaid_mappingfile_1) as f:
            reader = csv.reader( f )
            for row in reader:
                print( " ".join( row ) )

        csvman_metaidmapping2 = CSVManager(self.metaid_mappingfile_1)
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
        self.assertNotEqual(metaid_pre_mapping_2, csvman_metaidmapping2.get_value("doi:10.1002/mrc.2517"))
        self.assertEqual(csvman_metaidmapping2.get_value("doi:10.1002/mrc.2517"), metaid_post_mapping_2)

        #check that canc_to_new_mid csv file keeps track of the metaid-to-metaid remappings
        csvman_canctonew = CSVManager(self.canc_to_new_mappingfile_1)
        prev_metaid = list(metaid_pre_mapping_2)[0]
        self.assertEqual(csvman_canctonew.get_value(prev_metaid), metaid_post_mapping_2)

    def test_create_rdf_from_csv(self):
        print("5# TEST")
        if not exists(self.rdf_from_csv_output_dir):
            makedirs(self.rdf_from_csv_output_dir)

        elif exists(self.rdf_from_csv_output_dir):
            for files in os.listdir(self.rdf_from_csv_output_dir):
                path = os.path.join(self.rdf_from_csv_output_dir, files )
                try:
                    shutil.rmtree( path )
                except OSError:
                    os.remove( path )

        self.assertTrue(exists(self.rdf_from_csv_output_dir))
        self.assertEqual(len(os.listdir(self.rdf_from_csv_output_dir)), 0)

        create_rdf_from_csv(self.input_csv_add, self.input_csv_del, self.rdf_from_csv_output_dir)

        self.assertEqual(len(os.listdir(self.rdf_from_csv_output_dir)), 2)

        for subdir, dirs, files in os.walk(self.rdf_from_csv_output_dir):
            for dir in dirs:
                dir_path = self.rdf_from_csv_output_dir + "%s" % (sep) + dir
                self.assertTrue( len( os.listdir(dir_path)) > 0 )
                self.assertEqual( len( os.listdir(dir_path) ), 1 )

            for file in files:
                filepath = os.path.join( subdir, file )
                if filepath.endswith( ".ttl" ):
                    g = Graph()
                    g.parse(filepath, format="nt11")
                    for s, p, o in g:
                        self.assertTrue( o != self.excluded_metaid )



    def test_last_metaid_update(self): #controlla che sia autonoma
        print("4# TEST")

        if exists(self.output_dir_2):
            shutil.rmtree(self.output_dir_2)

        if exists(self.metaid_mappingfile_2):
            remove(self.metaid_mappingfile_2)
            header = ["id", "value"]
            with open(self.metaid_mappingfile_2, 'w', newline="" ) as midmcsv_file:
                writer = csv.writer( midmcsv_file )
                writer.writerow( header )

        if exists(self.canc_to_new_mappingfile_2):
            remove( self.canc_to_new_mappingfile_2)
            header = ["id", "value"]
            with open(self.canc_to_new_mappingfile_2, 'w', newline="" ) as canc_to_new_csv_file:
                writer = csv.writer(canc_to_new_csv_file)
                writer.writerow( header )

        if exists( self.last_metaid_file_2):
            remove(self.last_metaid_file_2)

        with open(self.last_metaid_file_2, 'w' ) as l_mid:
            l_mid.write( "0" )

        with open(self.last_metaid_file_2, "r" ) as l_mid:
            data = l_mid.read()
            match = re.search( '(\d+)', data )
            if match:
                last_metaid_prev = int( match.group(0))

        process(self.input_dir_cnc_csv_extra, self.input_dir_mappings, self.metaid_mappingfile_2,
                 self.canc_to_new_mappingfile_2, self.last_metaid_file_2, self.output_dir_2)

        g = Graph()
        for subdir, dirs, files in os.walk(self.output_dir_2):
            for file in files:
                filepath = os.path.join( subdir, file )
                if "new_mappings" in filepath:
                    new_triples_nt = filepath

        g.parse(new_triples_nt, format="nt11" )
        new_ids_mapped = 0
        for index, (s, p, o) in enumerate(g):
            new_ids_mapped += 1

        with open(self.last_metaid_file_2, "r" ) as l_mid:
            data = l_mid.read()
            match = re.search( '(\d+)', data )
            if match:
                last_metaid_cur = int( match.group(0))

        #check that the last metaid number has grown of as many unities as the number of the new mapped ids
        self.assertEqual(last_metaid_cur, (last_metaid_prev + new_ids_mapped))



if __name__ == '__main__':
    unittest.main()
#python -m unittest index.test.12_mapping
import pandas as pd
from argparse import ArgumentParser
from index.storer.csvmanager import CSVManager
from os import sep, makedirs, walk
from os.path import exists
from zipfile import ZipFile
from tarfile import TarFile
import re
import csv
import os

def get_all_files(i_dir):
    result = []
    opener = None

    if i_dir.endswith( ".zip" ):
        zf = ZipFile( i_dir )
        for name in zf.namelist():
            if name.lower().endswith(".csv") and "citations" not in name.lower() and "source" not in name.lower():
                result.append( name )
        opener = zf.open
    elif i_dir.endswith( ".tar.gz" ):
        tf = TarFile.open( i_dir )
        for name in tf.getnames():
            if name.lower().endswith(".csv") and "citations" not in name.lower() and "source" not in name.lower():
                result.append(name)
        opener = tf.extractfile
    else:
        for cur_dir, cur_subdir, cur_files in walk(i_dir):
            for file in cur_files:
                if file.lower().endswith( ".csv" ) and "citations" not in file.lower() and "source" not in file.lower():
                    result.append(cur_dir + sep + file)
        opener = open
    return result, opener


def process(input_dir1, input_dir2, midmcsv, output_dir):
    if not exists(output_dir):
        makedirs(output_dir)

    if not exists(midmcsv):
        header = ["id", "value"]
        with open(midmcsv, 'w' ) as midmcsv_file:
            writer = csv.writer(midmcsv_file)
            writer.writerow(header)

    metaid_mapping = CSVManager(midmcsv)
    all_cnc_input_files, opener = get_all_files(input_dir1)
    all_mapping_input_files, opener = get_all_files(input_dir2)
    len_all_cnc_files = len(all_cnc_input_files)
    len_all_mapping_files = len(all_mapping_input_files)

    dfmap = pd.read_csv(midmcsv)
    last_metaid = (dfmap["value"]).max()
    if pd.isnull(last_metaid):
        last_metaid_assigned = 0
    else:
        last_metaid_assigned = last_metaid
        print("last_metaid is:", last_metaid)

    # Read all the CSV file produced by cnc for a given Index
    metaids_to_reassign = set()
    for file_idx, file in enumerate( all_cnc_input_files, 1 ):
        df = pd.DataFrame()
        for chunk in pd.read_csv(file, chunksize=1000 ):
            f = pd.concat( [df, chunk], ignore_index=True )
            print( "CNC FILES: Open file %s of %s" % (file_idx, len_all_cnc_files) )
            for index, row in f.iterrows():

                oci_prefix = str(row["oci"])

                #to be extended in case new types of ids are handled in the mapping files
                if oci_prefix.startswith('0160'):
                    id_type = "pmid"
                elif oci_prefix.startswith('020'):
                    id_type = "doi"

                citing = id_type + ":" + str(row["citing"])
                cited = id_type + ":" + str(row["cited"])
                same_id_as = set()
                same_id_as_cited = set()

                if len_all_mapping_files >0:
                    for file_idx, filemap in enumerate(all_mapping_input_files, 1):
                        df = pd.DataFrame()
                        for chunk in pd.read_csv( filemap, chunksize=1000 ):
                            f = pd.concat( [df, chunk], ignore_index=True )
                            print( "MAPPING FILES: Open file %s of %s" % (file_idx, len_all_mapping_files) )
                            for index, row in f.iterrows():
                                #check presence of multiple ids for the same publication
                                if citing in row["id"] or citing in row["value"]:
                                    print("Multiple ids for same publication found")
                                    same_id_as.add(row["id"])
                                    same_id_as.add(row["value"])
                                else:
                                    print("No matched ids found for:", citing)

                                #check presence of multiple ids for the same publication (cited)
                                if cited in row["id"] or citing in row["value"]:
                                    print("Multiple ids for same publication found")
                                    same_id_as_cited.add(row["id"])
                                    same_id_as_cited.add(row["value"])
                                else:
                                    print("No matched ids found for:", cited)

                            if len(same_id_as) == 0:
                                same_id_as.add(citing)

                            if len(same_id_as_cited) == 0:
                                same_id_as_cited.add(cited)
                else:
                    same_id_as.add(citing)
                    same_id_as_cited.add( cited )

                #to collect metaids possibly previously assigned to multiple correspondant ids for the same publication
                previous_metaids = set()
                previous_metaids_cited = set()

                for id in same_id_as:
                    assigned_metaid = metaid_mapping.get_value(id)
                    if assigned_metaid is not None:
                        for id in assigned_metaid:
                            assigned_metaid.remove(id)
                            assigned_metaid.add(int(id))
                        previous_metaids.update(assigned_metaid)

                for idcited in same_id_as_cited:
                    assigned_metaid_cited = metaid_mapping.get_value(idcited)
                    if assigned_metaid_cited is not None:
                        for idcited in assigned_metaid_cited:
                            assigned_metaid_cited.remove(idcited)
                            assigned_metaid_cited.add(int(idcited))
                        previous_metaids_cited.update(assigned_metaid_cited)

                #Case1: only one metaid was assigned to one of the correspondant ids for the same publication: all
                # the other ids for the same publication must be assigned the same metaid.
                if len(previous_metaids) == 1:
                    metaid = str(list(previous_metaids)[0])
                    for id in same_id_as:
                        if metaid_mapping.get_value(id) is None:
                            metaid_mapping.add_value(id, metaid)
                        elif metaid_mapping.get_value(id) is not None and metaid not in metaid_mapping.get_value(id):
                            metaid_mapping.substitute_value(id, metaid)

                # Case2: more metaids were assigned to correspondant ids for the same publication: all
                # the ids for the same publication must be assigned/reassigned the same metaid, which is the lowest
                # among the assigned ones. The other metaids are removed from the mapping and reassigned to other ids.
                elif len(previous_metaids) > 1:
                    metaid = min(previous_metaids)
                    previous_metaids.remove(metaid)
                    metaids_to_reassign.update(previous_metaids)
                    metaid = str(metaid)

                    for id in same_id_as:
                        if metaid_mapping.get_value(id) is None:
                            metaid_mapping.add_value(id, metaid)
                        elif metaid_mapping.get_value(id) is not None and metaid not in metaid_mapping.get_value(id):
                            print("its current value is:", metaid_mapping.get_value( id ))
                            print("the value it should have:", metaid)
                            metaid_mapping.substitute_value(id, metaid)
                            print("its current value is:", metaid_mapping.get_value( id ))


                # Case3: No metaid found for correspondant ids for the same publication. If it is possible, one of the
                # discarted metaids is reassigned. If no metaid is to be reassigned, a new one is assigned.
                else:
                    if len(metaids_to_reassign) > 0:
                        metaid = min(metaids_to_reassign)
                        metaids_to_reassign.remove(metaid)
                        metaid = str(metaid)
                    else:
                        print("No metaids to reassign, Increase last_metaid_assigned:", last_metaid_assigned)
                        metaid = last_metaid_assigned + 1
                        last_metaid_assigned = metaid
                        metaid = str(metaid)

                    for id in same_id_as:
                        if metaid_mapping.get_value(id) is None:
                            metaid_mapping.add_value(id, metaid)
                        elif metaid_mapping.get_value(id) is not None and metaid not in metaid_mapping.get_value(id):
                            metaid_mapping.substitute_value(id, metaid)

                #The whole process is repeated for cited ids from cnc output

                # Case1: only one metaid was assigned to one of the correspondant ids for the same publication: all
                # the other ids for the same publication must be assigned the same metaid.
                if len( previous_metaids_cited ) == 1:
                    metaid = str( list( previous_metaids_cited )[0] )
                    for idcited in same_id_as_cited:
                        if metaid_mapping.get_value( idcited ) is None:
                            metaid_mapping.add_value( idcited, metaid )
                        elif metaid_mapping.get_value(idcited) is not None and metaid not in metaid_mapping.get_value(idcited):
                            metaid_mapping.substitute_value(idcited, metaid )

                # Case2: more metaids were assigned to correspondant ids for the same publication: all
                # the ids for the same publication must be assigned/reassigned the same metaid, which is the lowest
                # among the assigned ones. The other metaids are removed from the mapping and reassigned to other ids.
                elif len(previous_metaids_cited) > 1:
                    metaid = min(previous_metaids_cited)
                    previous_metaids_cited.remove(metaid)
                    metaids_to_reassign.update(previous_metaids_cited)
                    metaid = str(metaid)

                    for idcited in same_id_as_cited:
                        if metaid_mapping.get_value(idcited) is None:
                            metaid_mapping.add_value(idcited, metaid )
                        elif metaid_mapping.get_value(idcited) is not None and metaid not in metaid_mapping.get_value(idcited):
                            print( "its current value is:", metaid_mapping.get_value(idcited))
                            print( "the value it should have:", metaid )
                            metaid_mapping.substitute_value(idcited, metaid )
                            print( "its current value is:", metaid_mapping.get_value(idcited) )


                # Case3: No metaid found for correspondant ids for the same publication. If it is possible, one of the
                # discarted metaids is reassigned. If no metaid is to be reassigned, a new one is assigned.
                else:
                    if len(metaids_to_reassign) > 0:
                        metaid = min(metaids_to_reassign)
                        metaids_to_reassign.remove(metaid)
                        metaid = str(metaid)
                    else:
                        print( "No metaids to reassign, Increase last_metaid_assigned:",
                               last_metaid_assigned )
                        metaid = last_metaid_assigned + 1
                        last_metaid_assigned = metaid
                        metaid = str(metaid)

                    for idcited in same_id_as_cited:
                        if metaid_mapping.get_value(idcited) is None:
                            metaid_mapping.add_value(idcited, metaid)
                        elif metaid_mapping.get_value(idcited) is not None and metaid not in metaid_mapping.get_value(idcited):
                            metaid_mapping.substitute_value(idcited, metaid)




if __name__ == "__main__":
    arg_parser = ArgumentParser( "Global files creator for mapping METAIDs to other IDs",
                                 description="Process existent mapping files and cnc output, to map METAIDs to "
                                             "other IDs")
    arg_parser.add_argument( "-i1", "--input_dir1", dest="input_dir1", required=True,
                             help="Directory containing cnc CSV output of the index whose citations are to be "
                                  "processed")
    arg_parser.add_argument( "-i2", "--input_dir2", dest="input_dir2", required=True,
                             help="Directory containing CSV files mapping two types of ids each")
    arg_parser.add_argument( "-midmcsv", "--midmcsv", dest="midmcsv", required=True,
                             help="CSV file containing previously computed mappings METAIDs - other ids")
    arg_parser.add_argument( "-o", "--output_dir", dest="output_dir", required=True,
                             help="The directory where the (updated) file containing the mapping among METAIDs "
                                  "and other types of identifiers and the RDF file containing only the new "
                                  "mappings added are stored")

    args = arg_parser.parse_args()
    process(args.input_dir1, args.input_dir2, args.midmcsv, args.output_dir)

#python -m index.support.mapping -i1 "index/noci_test/data/csv" -i2 "index/test_data/nih_glob1/mapping" -midmcsv "index/test_data/mapping/metaidmapping.csv" -o "index/test_data/mapping"
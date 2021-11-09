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
from rdflib import Graph, URIRef
from rdflib.namespace import DCTERMS
from csv import reader
from datetime import datetime
import random

#Access all input file needed, contained in a specified directory
def get_all_files(i_dir):
    result = []
    opener = None

    if i_dir.endswith( ".zip" ):
        zf = ZipFile( i_dir )
        for name in zf.namelist():
            if name.lower().endswith(".csv"):
                result.append(name)
        opener = zf.open
    elif i_dir.endswith( ".tar.gz" ):
        tf = TarFile.open( i_dir )
        for name in tf.getnames():
            if name.lower().endswith(".csv"):
                result.append(name)
        opener = tf.extractfile
    else:
        for cur_dir, cur_subdir, cur_files in walk(i_dir):
            for file in cur_files:
                if file.lower().endswith( ".csv" ):
                    result.append(cur_dir + sep + file)
        opener = open
    return result, opener

#Create a nt files from a csv files
def create_rdf_from_csv(csvfile_add, csvfile_delete):
    g = Graph()
    gdel = Graph()

    #Set prefixes for DOIs, PMIDs and METAIDs, and store in a variable the only preicate which will be used
    doi_uri_pref = "http://dx.doi.org/"
    pmid_uri_pref = "https://pubmed.ncbi.nlm.nih.gov/"
    metaid_uri_pref = "https://w3id.org/oc/meta/br/060"
    predicate = DCTERMS.relation
    cur_date = datetime.today().strftime( '%Y-%m-%d' )

    #Read the csv file skipping the header line
    with open(csvfile_delete, 'r' ) as deletefile:
        del_existing_lines = [line for line in reader(deletefile, delimiter=',' )]

    discarded_rows = []
    with open(csvfile_add, 'r' ) as addfile:
        reader_addfile = reader( addfile, delimiter=',' )
        #next(reader_addfile)
        for row in reader_addfile:
            if row in del_existing_lines:
                discarded_rows.append(row)
            elif row:
                # store in the id variable the identifier (doi or pmid) without its prefix, and use it in the id's URI
                id = row[0].strip()
                if id.startswith("pmid:"):
                    id_uri =URIRef(pmid_uri_pref+id[5:])
                elif id.startswith("doi:"):
                    id_uri =URIRef(doi_uri_pref+id[4:])
                metaid_uri = URIRef(metaid_uri_pref+row[1])
                #Add the triple to the graph
                if id_uri and metaid_uri:
                    g.add((id_uri, predicate, metaid_uri))
                    print("added triple:", id_uri, predicate, metaid_uri)

        #Save the addition graph in a file containing the date of creation and the name of the original csv file
        if csvfile_add.endswith('.csv'):
            match = re.search(".+?(?=\.csv)", csvfile_add)
            if match:
                filename = match.group(0)
        else:
            filename = csvfile_add
        rdf_filename = filename + "_" + cur_date + ".nt"
        g.serialize(destination= rdf_filename, format='nt')

    print("discarded rows", discarded_rows)
    for row in del_existing_lines:
        if row and row not in discarded_rows:
            # store in the id variable the identifier (doi or pmid) without its prefix, and use it in the id's URI
            id = row[0].strip()
            if id.startswith( "pmid:" ):
                id_uri = URIRef( pmid_uri_pref + id[5:] )
            elif id.startswith( "doi:" ):
                id_uri = URIRef( doi_uri_pref + id[4:] )
            metaid_uri = URIRef( metaid_uri_pref + row[1] )
            # Add the triple to the graph
            if id_uri and metaid_uri:
                gdel.add( (id_uri, predicate, metaid_uri) )
                print( "added triple to delete file:", id_uri, predicate, metaid_uri )

    # Save the deletion graph in a file containing the date of creation and the name of the original csv file
    if csvfile_delete.endswith( '.csv' ):
        match = re.search( ".+?(?=\.csv)", csvfile_delete )
        if match:
            filename = match.group( 0 )
    else:
        filename = csvfile_delete
    rdf_filename = filename + "_" + cur_date + ".nt"
    gdel.serialize( destination=rdf_filename, format='nt' )


"""
process is the main function. It takes in input:
 1) the directory where the input files (output of cnc) are stored,
 2) the directory where the mapping files are stored (in this case, output of nocimapping.py, developed for NIH data)
 3) a csv file containing previous mapping between ids and metaids (in case it is the first run, the file is generated)
 4) a csv file mapping between previously assigned (now invalidated) metaids and new metaids. This is useful in those
    cases when new mapping data reveal that some ids which were previously assigned different metaids point to the 
    same object. In the case the file doesn't exist, it is generated. 
 5) a txt file containing a string stating the last metaid assigned. At the first run of this code this file can either
    be initalised with the uri of the metaid "0" (i.e.: "https://w3id.org/oc/meta/br/0600") or - in case it doesn't 
    exist - be created on the fly. 
 6) an output directory where the main output file are stored, i.e.:
    a) a .nt file containing the triples to delete
    b) a .nt file containing the triples to add
 
The output material includes:
    a) the two .nt files mentioned at point 6) 
    b) a .csv file with the ids in the first column and the metaid associated in the second. This file is the one
        mentioned at point 3), which is generated in the first code run, and updated in all the other cases.   
    c) a .csv file with the invalidated metaids in the first column and the correspondant metaids they were redirected
        to in in the second. This file is the one mentioned at point 4), which is generated in the first code run, and 
        updated in all the other cases.
    d) An updated version of a txt file (mentioned at point 5)) containing the string of the last metaid assigned. 
"""

def process(input_dir1, input_dir2, midmcsv, canc_to_new_csv, last_metaid, output_dir):
    if not exists(output_dir):
        makedirs(output_dir)

    if not exists(midmcsv):
        header = ["id", "value"]
        with open(midmcsv, 'w', newline="") as midmcsv_file:
            writer = csv.writer(midmcsv_file)
            writer.writerow(header)


    if not exists(canc_to_new_csv):
        header = ["id", "value"]
        with open(canc_to_new_csv, 'w', newline="") as canc_to_new_csv_file:
            writer = csv.writer(canc_to_new_csv_file)
            writer.writerow(header)

    if not exists(last_metaid):
        with open(last_metaid, 'w' ) as l_mid:
            l_mid.write("0")

    with open(last_metaid, "r") as l_mid:
        data = l_mid.read()
        match = re.search( '(\d+)', data )
        if match:
            next_metaid = int(match.group(0))+1
            print("next_metaid", next_metaid)


    metaid_mapping = CSVManager(midmcsv)
    canc_to_new_mapping = CSVManager(canc_to_new_csv)

    new_mappings = output_dir + sep + "new_mappings.csv"
    if not exists(new_mappings):
        header = ["id", "value"]
        with open(new_mappings, 'w' ) as new_mappings_file:
            writer = csv.writer(new_mappings_file)
            writer.writerow(header)
    new_mappings_csv = CSVManager(new_mappings)

    deleted_mappings = output_dir + sep + "deleted_mappings.csv"
    if not exists(deleted_mappings):
        header = ["id", "value"]
        with open(deleted_mappings, 'w' ) as deleted_mappings_file:
            writer = csv.writer(deleted_mappings_file)
            writer.writerow(header)
    deleted_mappings_csv = CSVManager(deleted_mappings)

    all_cnc_input_files, opener = get_all_files(input_dir1)
    all_mapping_input_files, opener = get_all_files(input_dir2)
    len_all_cnc_files = len(all_cnc_input_files)
    len_all_mapping_files = len(all_mapping_input_files)

    # Read all the CSV file produced by cnc for a given Index
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
                if metaid_mapping.get_value(citing):
                    pass_citing = True
                else:
                    pass_citing = False

                cited = id_type + ":" + str(row["cited"])
                if metaid_mapping.get_value(cited):
                    pass_cited = True
                else:
                    pass_cited = False

                print("Processing citation:", citing, cited)
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
                                    print("No multiple ids found for:", citing)

                                #check presence of multiple ids for the same publication (cited)
                                if cited in row["id"] or cited in row["value"]:
                                    print("Multiple ids for same publication found")
                                    same_id_as_cited.add(row["id"])
                                    same_id_as_cited.add(row["value"])
                                else:
                                    print("No multiple ids found for:", cited)

                            if len(same_id_as) == 0:
                                same_id_as.add(citing)

                            if len(same_id_as_cited) == 0:
                                same_id_as_cited.add(cited)
                else:
                    same_id_as.add(citing)
                    same_id_as_cited.add(cited)

                casual_id = random.sample(same_id_as,1)
                casual_value = metaid_mapping.get_value((casual_id)[0])
                if pass_citing and len(same_id_as) == 1:
                    pass
                elif len(same_id_as)>1 and all(metaid_mapping.get_value(i) == casual_value for i in same_id_as):
                    pass
                else:
                    #to collect metaids previously assigned to multiple correspondant ids for the same publication
                    previous_metaids = set()
                    for id in same_id_as:
                        assigned_metaid = metaid_mapping.get_value(id)
                        if assigned_metaid is not None:
                            for id in assigned_metaid:
                                #verify the type of the id
                                assigned_metaid.remove(id)
                                assigned_metaid.add(int(id))
                            previous_metaids.update(assigned_metaid)

                    # Case1: only one metaid was assigned to one of the correspondant ids for the same publication: all
                    # the other ids for the same publication must be assigned the same metaid.
                    if len( previous_metaids ) == 1:
                        metaid = str( list( previous_metaids )[0] )
                        for id in same_id_as:
                            print( "id:", id )
                            if metaid_mapping.get_value( id ) is None:
                                metaid_mapping.add_value( id, metaid )
                                new_mappings_csv.add_value( id, metaid )

                            elif metaid_mapping.get_value(
                                    id ) is not None and metaid not in metaid_mapping.get_value( id ):

                                for val in metaid_mapping.get_value( id ):
                                    canc_to_new_mapping.add_value( val, metaid )
                                    deleted_mappings_csv.add_value( id, val )
                                metaid_mapping.substitute_value( id, metaid )
                                new_mappings_csv.add_value( id, metaid )

                            else:

                                for val in metaid_mapping.get_value( id ):
                                    if val != metaid:
                                        canc_to_new_mapping.add_value( val, metaid )
                                        deleted_mappings_csv.add_value( id, val )
                                if metaid_mapping.get_value( id ) != {metaid}:
                                    metaid_mapping.substitute_value( id, metaid )
                                    new_mappings_csv.add_value( id, metaid )


                    # Case2: more metaids were assigned to correspondant ids for the same publication: all
                    # the ids for the same publication must be assigned/reassigned the same metaid, which is the lowest
                    # among the assigned ones. The other metaids are invalidated.
                    elif len( previous_metaids ) > 1:
                        metaid = min( previous_metaids )
                        previous_metaids.remove( metaid )
                        metaid = str( metaid )

                        for id in same_id_as:
                            if metaid_mapping.get_value( id ) is None:
                                metaid_mapping.add_value( id, metaid )
                                new_mappings_csv.add_value( id, metaid )

                            elif metaid_mapping.get_value(id ) is not None and metaid not in metaid_mapping.get_value( id ):

                                for val in metaid_mapping.get_value( id ):
                                    canc_to_new_mapping.add_value( val, metaid )
                                    deleted_mappings_csv.add_value( id, val )
                                metaid_mapping.substitute_value( id, metaid )
                                new_mappings_csv.add_value( id, metaid )

                            else:
                                for val in metaid_mapping.get_value( id ):
                                    if val != metaid:
                                        canc_to_new_mapping.add_value( val, metaid )
                                        deleted_mappings_csv.add_value( id, val )
                                if metaid_mapping.get_value( id ) != {metaid}:
                                    metaid_mapping.substitute_value( id, metaid )
                                    new_mappings_csv.add_value( id, metaid )

                    # Case3: No metaid found for correspondant ids for the same publication. A new metaid is assigned.

                    else:
                        metaid = str( next_metaid )
                        next_metaid += 1
                        with open( last_metaid, "r" ) as l_mid:
                            data = l_mid.read()
                            match = re.search( '(\d+)', data)
                            if match:
                                ret = (re.sub( match.group( 0 ), metaid, data))
                        with open( last_metaid, "w" ) as l_mid:
                            l_mid.write( ret )

                        for id in same_id_as:
                            if metaid_mapping.get_value( id ) is None:
                                metaid_mapping.add_value( id, metaid )
                                new_mappings_csv.add_value( id, metaid )

                            elif metaid_mapping.get_value(id ) is not None and metaid not in metaid_mapping.get_value( id ):

                                for val in metaid_mapping.get_value( id ):
                                    canc_to_new_mapping.add_value( val, metaid )
                                    deleted_mappings_csv.add_value( id, val )
                                metaid_mapping.substitute_value( id, metaid )
                                new_mappings_csv.add_value( id, metaid )
                            else:
                                for val in metaid_mapping.get_value( id ):
                                    if val != metaid:
                                        canc_to_new_mapping.add_value( val, metaid )
                                        deleted_mappings_csv.add_value( id, val )
                                if metaid_mapping.get_value( id ) != {metaid}:
                                    metaid_mapping.substitute_value( id, metaid )
                                    new_mappings_csv.add_value( id, metaid )

                casual_id_cited = random.sample(same_id_as_cited,1)
                casual_value = metaid_mapping.get_value((casual_id_cited)[0])

                if pass_cited and len(same_id_as_cited) == 1:
                    pass
                elif len(same_id_as_cited)>1 and all(metaid_mapping.get_value(i) == casual_value for i in same_id_as_cited):
                    pass
                else:
                    previous_metaids_cited = set()
                    for idcited in same_id_as_cited:
                        assigned_metaid_cited = metaid_mapping.get_value(idcited)
                        if assigned_metaid_cited is not None:
                            for idcited in assigned_metaid_cited:
                                assigned_metaid_cited.remove(idcited)
                                assigned_metaid_cited.add(int(idcited))
                            previous_metaids_cited.update(assigned_metaid_cited)


                    #The whole process is repeated for cited ids from cnc output

                    # Case1: only one metaid was assigned to one of the correspondant ids for the same publication: all
                    # the other ids for the same publication must be assigned the same metaid.
                    if len(previous_metaids_cited) == 1:
                        metaid = str(list(previous_metaids_cited)[0])
                        for id_cited in same_id_as_cited:
                            if metaid_mapping.get_value(id_cited) is None:
                                metaid_mapping.add_value( id_cited, metaid )
                                new_mappings_csv.add_value( id_cited, metaid )
                            elif metaid_mapping.get_value( id_cited ) is not None and metaid not in metaid_mapping.get_value(id_cited):
                                for val in metaid_mapping.get_value(id_cited):
                                    canc_to_new_mapping.add_value( val, metaid )
                                    deleted_mappings_csv.add_value( id_cited, val )
                                metaid_mapping.substitute_value(id_cited, metaid)
                                new_mappings_csv.add_value(id_cited, metaid )
                            else:
                                for val in metaid_mapping.get_value( id_cited ):
                                    if val != metaid:
                                        canc_to_new_mapping.add_value( val, metaid )
                                        deleted_mappings_csv.add_value(id_cited, val)
                                if metaid_mapping.get_value(id_cited) != {metaid}:
                                    metaid_mapping.substitute_value(id_cited, metaid)
                                    new_mappings_csv.add_value(id_cited, metaid )

                    # Case2: more metaids were assigned to correspondant ids for the same publication: all
                    # the ids for the same publication must be assigned/reassigned the same metaid, which is the lowest
                    # among the assigned ones. The other metaids are invalidated.
                    elif len(previous_metaids_cited) > 1:
                        metaid = min(previous_metaids_cited)
                        previous_metaids_cited.remove( metaid )
                        metaid = str(metaid)

                        for id_cited in same_id_as_cited:
                            if metaid_mapping.get_value( id_cited ) is None:
                                metaid_mapping.add_value( id_cited, metaid )
                                new_mappings_csv.add_value(id_cited, metaid )
                            elif metaid_mapping.get_value( id_cited ) is not None and metaid not in metaid_mapping.get_value(id_cited):
                                for val in metaid_mapping.get_value(id_cited):
                                    canc_to_new_mapping.add_value( val, metaid )
                                    deleted_mappings_csv.add_value(id_cited, val)
                                metaid_mapping.substitute_value(id_cited, metaid )
                                new_mappings_csv.add_value( id_cited, metaid )
                            else:
                                for val in metaid_mapping.get_value( id_cited ):
                                    if val != metaid:
                                        canc_to_new_mapping.add_value( val, metaid )
                                        deleted_mappings_csv.add_value( id_cited, val )
                                if metaid_mapping.get_value(id_cited) != {metaid}:
                                    metaid_mapping.substitute_value( id_cited, metaid )
                                    new_mappings_csv.add_value( id_cited, metaid )

                    # Case3: No metaid found for correspondant ids for the same publication. A new metaid is assigned.
                    else:
                        metaid = str( next_metaid )
                        next_metaid += 1
                        with open( last_metaid, "r" ) as l_mid:
                            data = l_mid.read()
                            match = re.search( '(\d+)', data)
                            if match:
                                ret = (re.sub( match.group( 0 ), metaid, data))
                        with open(last_metaid, "w") as l_mid:
                            l_mid.write(ret)

                        for id_cited in same_id_as_cited:
                            if metaid_mapping.get_value(id_cited) is None:
                                metaid_mapping.add_value(id_cited, metaid )
                                new_mappings_csv.add_value(id_cited, metaid )
                            elif metaid_mapping.get_value(id_cited) is not None and metaid not in metaid_mapping.get_value(id_cited):
                                for val in metaid_mapping.get_value(id_cited):
                                    canc_to_new_mapping.add_value( val, metaid )
                                    deleted_mappings_csv.add_value(id_cited, val)
                                metaid_mapping.substitute_value(id_cited, metaid )
                                new_mappings_csv.add_value(id_cited, metaid)
                            else:
                                for val in metaid_mapping.get_value( id_cited ):
                                    if val != metaid:
                                        canc_to_new_mapping.add_value( val, metaid )
                                        deleted_mappings_csv.add_value( id_cited, val )
                                if metaid_mapping.get_value(id_cited) != {metaid}:
                                    metaid_mapping.substitute_value( id_cited, metaid )
                                    new_mappings_csv.add_value( id_cited, metaid )

    create_rdf_from_csv(new_mappings, deleted_mappings)
    os.remove(new_mappings)
    os.remove(deleted_mappings)


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
    arg_parser.add_argument( "-ctncsv", "--ctncsv", dest="ctncsv", required=True,
                             help="file containing the mapping between deleted metaids and metaids they were "
                                  "redirected to")
    arg_parser.add_argument( "-lmid", "--lmid", dest="lmid", required=True,
                             help="text file containing the string representing the last metaid assigned")
    arg_parser.add_argument( "-o", "--output_dir", dest="output_dir", required=True,
                             help="The directory where the (updated) file containing the mapping among METAIDs "
                                  "and other types of identifiers and the RDF file containing only the new "
                                  "mappings added are stored")

    args = arg_parser.parse_args()
    process(args.input_dir1, args.input_dir2, args.midmcsv, args.ctncsv, args.lmid, args.output_dir)

#python -m index.mapping_global.mapping -i1 "index/noci_test/data/csv" -i2 "index/mapping_global/NIH_mapping" -midmcsv "index/mapping_global/metaidmapping.csv" -ctncsv "index/mapping_global/canc_to_new_mid.csv" -lmid "index/mapping_global/last_metaid.txt" -o "index/test_data/mapping"
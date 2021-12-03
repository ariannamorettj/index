#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2019,
# Ivan Heibi <ivanhb.ita@gmail.com>
# Silvio Peroni <essepuntato@gmail.com>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.
import pandas as pd
from argparse import ArgumentParser
from index.storer.csvmanager import CSVManager
from index.finder.crossrefresourcefinder import CrossrefResourceFinder
from index.finder.orcidresourcefinder import ORCIDResourceFinder
from index.identifier.pmidmanager import PMIDManager
from index.identifier.doimanager import DOIManager
from index.identifier.issnmanager import ISSNManager
from index.identifier.orcidmanager import ORCIDManager
from os import sep, makedirs, walk
import os
from os.path import exists
import csv
import json
from collections import Counter
from re import sub
from index.citation.oci import Citation
from zipfile import ZipFile
from tarfile import TarFile
import re

def issn_data_recover(directory):
    journal_issn_dict = dict()
    filename = directory + sep + 'journal_issn.json'
    if not os.path.exists(filename):
        return journal_issn_dict
    else:
        with open(filename, 'r', encoding='utf8') as fd:
            journal_issn_dict = json.load(fd)
            types = type(journal_issn_dict)
            return journal_issn_dict

def issn_data_to_cache(name_issn_dict, directory):
    filename = directory + sep + 'journal_issn.json'
    with open(filename, 'w', encoding='utf-8' ) as fd:
            json.dump(name_issn_dict, fd, ensure_ascii=False, indent=4)

#PUB DATE EXTRACTION : should take in input a data structure representing one publication
def build_pubdate(row):
    year = str(row["year"])
    str_year = sub( "[^\d]", "", year)[:4]
    if str_year:
        return str_year
    else:
        return None


# Function aimed at extracting all the needed files for the input directory
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


def process(input_dir, output_dir, n):
    if not exists(output_dir):
        makedirs(output_dir)

    citing_pmid_with_no_date = set()
    valid_pmid = CSVManager( output_dir + sep + "valid_pmid.csv" )
    valid_doi = CSVManager("index/test_data/crossref_glob" + sep + "valid_doi.csv")
    id_date = CSVManager( output_dir + sep + "id_date_pmid.csv" )
    id_issn = CSVManager( output_dir + sep + "id_issn_pmid.csv" )
    id_orcid = CSVManager( output_dir + sep + "id_orcid_pmid.csv" )
    journal_issn_dict = issn_data_recover(output_dir) #just an empty dict in case the code never broke
    pmid_manager = PMIDManager(valid_pmid)
    crossref_resource_finder = CrossrefResourceFinder(valid_doi)
    orcid_resource_finder = ORCIDResourceFinder(valid_doi)

    doi_manager = DOIManager(valid_doi)
    issn_manager = ISSNManager()
    orcid_manager = ORCIDManager()

    all_files, opener = get_all_files(input_dir)
    len_all_files = len(all_files)

    # Read all the CSV file in the NIH dump to create the main information of all the indexes
    print( "\n\n# Add valid PMIDs from NIH metadata" )
    for file_idx, file in enumerate( all_files, 1 ):
        df = pd.DataFrame()

        for chunk in pd.read_csv(file, chunksize=1000 ):
            f = pd.concat( [df, chunk], ignore_index=True )
            f.fillna("", inplace=True)

            print( "Open file %s of %s" % (file_idx, len_all_files) )
            for index, row in f.iterrows():
                if int(index) !=0 and int(index) % int(n) == 0:
                    print( "Group nr.", int(index)//int(n), "processed. Data from", int(index), "rows saved to journal_issn.json mapping file")
                    issn_data_to_cache(journal_issn_dict, output_dir)

                citing_pmid = pmid_manager.normalise(row['pmid'], True)
                pmid_manager.set_valid(citing_pmid)
                citing_doi = doi_manager.normalise(row['doi'], True)
                #doi_manager.set_valid(citing_doi)

                if id_date.get_value(citing_pmid) is None:
                    citing_date = Citation.check_date(build_pubdate(row))
                    if citing_date is not None:
                        id_date.add_value(citing_pmid, citing_date)
                        if citing_pmid in citing_pmid_with_no_date:
                            citing_pmid_with_no_date.remove(citing_pmid)
                    else:
                        citing_pmid_with_no_date.add( citing_pmid )

                if id_issn.get_value( citing_pmid ) is None:
                    journal_name = row["journal"]
                    if journal_name: #check that the string is not empty
                        if journal_name in journal_issn_dict.keys():
                            for issn in journal_issn_dict[journal_name]:
                                id_issn.add_value(citing_pmid, issn)
                        else:
                            if citing_doi is not None:
                                json_res = crossref_resource_finder._call_api(citing_doi)
                                if json_res is not None:
                                    issn_set = crossref_resource_finder._get_issn(json_res)
                                    if len(issn_set)>0:
                                        journal_issn_dict[journal_name] = []
                                    for issn in issn_set:
                                        issn_norm = issn_manager.normalise(str(issn))
                                        id_issn.add_value( citing_pmid, issn_norm )
                                        journal_issn_dict[journal_name].append(issn_norm)


                if id_orcid.get_value(citing_pmid) is None:
                    if citing_doi is not None:
                        json_res = orcid_resource_finder._call_api(citing_doi)
                        if json_res is not None:
                            orcid_set = orcid_resource_finder._get_orcid(json_res)
                            for orcid in orcid_set:
                                orcid_norm = orcid_manager.normalise( orcid )
                                id_orcid.add_value(citing_pmid, orcid_norm)
            issn_data_to_cache( journal_issn_dict, output_dir )


    # Iterate once again for all the rows of all the csv files, so to check the validity of the referenced pmids.
    print( "\n\n# Checking the referenced pmids validity" )
    for file_idx, file in enumerate( all_files, 1 ):
        df = pd.DataFrame()

        for chunk in pd.read_csv( file, chunksize=1000 ):
            f = pd.concat( [df, chunk], ignore_index=True )
            f.fillna("", inplace=True)
            print( "Open file %s of %s" % (file_idx, len_all_files) )
            for index, row in f.iterrows():
                if row["references"] != "":
                    ref_string = row["references"].strip()
                    ref_string_norm = re.sub("\s+", " ", ref_string)
                else:
                    print("the type of row reference is", (row["references"]), type(row["references"]))
                    print(index, row )

                cited_pmids = set(ref_string_norm.split(" "))
                for cited_pmid in cited_pmids:
                    if pmid_manager.is_valid(cited_pmid):
                        print("valid cited pmid added:", cited_pmid)
                    else:
                        print("invalid cited pmid discarded:", cited_pmid)

    #Check if it is correct to do it also in this case
    for pmid in citing_pmid_with_no_date:
        id_date.add_value( pmid, "" )

if __name__ == "__main__":
    arg_parser = ArgumentParser( "Global files creator for NOCI",
                                 description="Process NIH CSV files and create global indexes to enable "
                                             "the creation of NOCI." )
    arg_parser.add_argument( "-i", "--input_dir", dest="input_dir", required=True,
                             help="Either the directory or the zip file that contains the NIH data dump "
                                  "of CSV files." )
    arg_parser.add_argument( "-o", "--output_dir", dest="output_dir", required=True,
                             help="The directory where the indexes are stored." )


    arg_parser.add_argument( "-n", "--num_lines", dest="n", required=True,
                             help="Number of lines after which the data stored in the dictionary for the mapping "
                                  "between a Journal name and the related issns are passed into a JSON cache file" )


    args = arg_parser.parse_args()
    process(args.input_dir, args.output_dir, args.n)


#python -m index.noci.glob1 -i "index/test_data/nih_dump" -o "index/test_data/nih_glob1" -n 20
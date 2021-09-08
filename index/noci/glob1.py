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
from os.path import exists
import csv
from collections import Counter
from re import sub
from index.citation.oci import Citation
from zipfile import ZipFile
from tarfile import TarFile


#PUB DATE EXTRACTION : should take in input a data structure representing one publication
def build_pubdate(row):
    year = str(row["year"])
    str_year = sub( "[^\d]", "", year)[:4]
    if str_year:
        print("build_pubdate:", str_year)
        return str_year
    else:
        print("build_pubdate:", None)
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
        print( "The OPENER is:", opener )
    print("it is returning:", result, opener) # ['index/test_data/nih_dump\\1.csv', 'index/test_data/nih_dump\\2.csv'] <built-in function open>
    return result, opener


def process(input_dir, output_dir):
    if not exists(output_dir):
        makedirs(output_dir)

    citing_pmid_with_no_date = set()
    valid_pmid = CSVManager( output_dir + sep + "valid_pmid.csv" )
    valid_doi = CSVManager("index/test_data/crossref_glob" + sep + "valid_doi.csv")
    doi_pmid_mapping = CSVManager( output_dir + sep + "pmid_to_doi.csv" )
    id_date = CSVManager( output_dir + sep + "id_date_pmid.csv" )
    id_issn = CSVManager( output_dir + sep + "id_issn_pmid.csv" )
    id_orcid = CSVManager( output_dir + sep + "id_orcid_pmid.csv" )

    journal_name_issn = CSVManager( output_dir + sep + "journal_name_issn.csv" )
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
        f = pd.read_csv(file)
        print( "Open file %s of %s" % (file_idx, len_all_files) )
        for index, row in f.iterrows():
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
                if journal_name_issn.get_value(journal_name) is not None:
                    for issn in journal_name_issn.get_value(journal_name):
                        id_issn.add_value( citing_pmid, issn )
                else:
                    if citing_doi is not None:
                        json_res = crossref_resource_finder._call_api(citing_doi)
                        if json_res is not None:
                            issn_set = crossref_resource_finder._get_issn(json_res)
                            print("issn_set", issn_set)
                            issn_set_norm = set()
                            for issn in issn_set:
                                issn_norm = issn_manager.normalise( str( issn ) )
                                id_issn.add_value( citing_pmid, issn_norm )
                                issn_set_norm.add( issn_norm )
                            if len( issn_set_norm ) > 0:
                                journal_name_issn.add_value( journal_name, str( issn_set_norm ) )

            if id_orcid.get_value(citing_pmid) is None:
                if citing_doi is not None:
                    json_res = orcid_resource_finder._call_api(citing_doi)
                    if json_res is not None:
                        orcid_set = orcid_resource_finder._get_orcid(json_res)
                        for orcid in orcid_set:
                            orcid_norm = orcid_manager.normalise( orcid )
                            id_orcid.add_value(citing_pmid, orcid_norm)

if __name__ == "__main__":
    arg_parser = ArgumentParser( "Global files creator for NOCI",
                                 description="Process NIH CSV files and create global indexes to enable "
                                             "the creation of NOCI." )
    arg_parser.add_argument( "-i", "--input_dir", dest="input_dir", required=True,
                             help="Either the directory or the zip file that contains the NIH data dump "
                                  "of CSV files." )
    arg_parser.add_argument( "-o", "--output_dir", dest="output_dir", required=True,
                             help="The directory where the indexes are stored." )

    args = arg_parser.parse_args()
    process(args.input_dir, args.output_dir)


#python -m index.noci.glob1 -i "index/test_data/nih_dump" -o "index/test_data/nih_glob1"

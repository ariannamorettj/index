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

from argparse import ArgumentParser
from index.storer.csvmanager import CSVManager
from index.identifier.doimanager import DOIManager
from index.identifier.issnmanager import ISSNManager
from index.identifier.orcidmanager import ORCIDManager
from os import sep, makedirs, walk
from os.path import exists
from json import load
from collections import Counter
from datetime import date
from re import sub
from index.citation.oci import Citation
from zipfile import ZipFile
from tarfile import TarFile
import codecs


"""
This function extracts the pubdate of a publication from the json object received in input, whose structure is:
{"items": [{
		"reference-count": 40, ...},
		{"references-count": 27, ...},
		{"reference-count": 29, ...}
    ]}
    
Each subdictionary representing a publication has the following structure:
	{
		"indexed": {
			"date-parts": [
				[2019, 9, 14]
			],
			"date-time": "2019-09-14T05:23:15Z",
			"timestamp": 1568438595866
		},
		"reference-count": 29,
		"publisher": "Elsevier BV",
		"license": [{
			"URL": "https:\/\/www.elsevier.com\/tdm\/userlicense\/1.0\/",
			"start": {
				"date-parts": [
					[2012, 12, 1]
				],
				"date-time": "2012-12-01T00:00:00Z",
				"timestamp": 1354320000000
			},
			"delay-in-days": 0,
			"content-version": "tdm"
		}],
		"funder": [{
			"name": "JISC (Joint Informations Systems Committee)",
			"award": []
		}],
		"content-domain": {
			"domain": [],
			"crossmark-restriction": false
		},
		"short-container-title": ["Journal of Web Semantics"],
		"published-print": {
			"date-parts": [
				[2012, 12]
			]
		},
		"DOI": "10.1016\/j.websem.2012.08.001",
		"type": "journal-article",
		"created": {
			"date-parts": [
				[2012, 8, 13]
			],
			"date-time": "2012-08-13T16:04:47Z",
			"timestamp": 1344873887000
		},
		"page": "33-43",
		"source": "Crossref",
		"is-referenced-by-count": 69,
		"title": ["FaBiO and CiTO: Ontologies for describing bibliographic resources and citations"],
		"prefix": "10.1016",
		"volume": "17",
		"author": [{
			"given": "Silvio",
			"family": "Peroni",
			"sequence": "first",
			"affiliation": []
		}, {
			"given": "David",
			"family": "Shotton",
			"sequence": "additional",
			"affiliation": []
		}],
		"member": "78",
		"container-title": ["Journal of Web Semantics"],
		"original-title": [],
		"language": "en",
		"link": [{
			"URL": "https:\/\/api.elsevier.com\/content\/article\/PII:S1570826812000790?httpAccept=text\/xml",
			"content-type": "text\/xml",
			"content-version": "vor",
			"intended-application": "text-mining"
		}, {
			"URL": "https:\/\/api.elsevier.com\/content\/article\/PII:S1570826812000790?httpAccept=text\/plain",
			"content-type": "text\/plain",
			"content-version": "vor",
			"intended-application": "text-mining"
		}],
		"deposited": {
			"date-parts": [
				[2019, 7, 2]
			],
			"date-time": "2019-07-02T20:24:32Z",
			"timestamp": 1562099072000
		},
		"score": 1.0,
		"subtitle": [],
		"short-title": [],
		"issued": {
			"date-parts": [
				[2012, 12]
			]
		},
		"references-count": 29,
		"alternative-id": ["S1570826812000790"],
		"URL": "http:\/\/dx.doi.org\/10.1016\/j.websem.2012.08.001",
		"relation": {},
		"ISSN": ["1570-8268"],
		"issn-type": [{
			"value": "1570-8268",
			"type": "print"
		}],
		"subject": ["Human-Computer Interaction", "Computer Networks and Communications", "Software"]
	}
"""

#FUNCTION FOR EXTRACTING THE DATE OF PUBLICATION. Takes in input a subdictionary of a Json object, representing a
# publication
def build_pubdate(obj):
    #First of all, it checks if the field "issued" is present in the dictionary. This field contains a dictionary
    #with the following shape: "issued": { #"date-parts": [[2012, 12]]}
    if 'issued' in obj:  # Main citing object
        if 'date-parts' in obj['issued']:
            # is an array of parts of dates
            try:
                #takes into account the first element of the array, which is always a list, containing the integers
                #representing the year, the month and the day of publication
                obj_date = obj['issued']['date-parts'][0]

                # lisdate[year,month,day]
                listdate = [1, 1, 1] #initialisation of the year, month and day values
                dateparts = []
                for i in range(0, len(obj_date)): #the number of elements should be either 1 or 2 or 3
                    try:
                        #attempt to add to the empty list each element contained in the given list (turned into an
                        # integer in case it came as a string
                        dateparts.append( obj_date[i] )
                        intvalue = int( obj_date[i] )
                        listdate[i] = intvalue #substitution of each initialised integer (i.e.=1) with the integer
                        # representing the known data (year, month and day)
                        print("listdate & obj_date & dateparts", listdate, obj_date, dateparts)
                        """
Progressive reconstruction of the date in the correct ultimate format:

1) listdate & obj_date & dateparts [2018, 1, 1] [2018, 2, 13] [2018]
2) listdate & obj_date & dateparts [2018, 2, 1] [2018, 2, 13] [2018, 2]
3) listdate & obj_date & dateparts [2018, 2, 13] [2018, 2, 13] [2018, 2, 13]

[2018, 2, 13], i.e. listdate in the shape obtained at the end of the loop is what we will use to generate the date

                        """
                    except:
                        pass

                # I have a date, so generate it
                if (1 < listdate[0] < 3000) and (0 < listdate[1] <= 12) and (0 < listdate[2] <= 31):
                    #checking that the numerical values are plausible for year, month and day
                    date_val = date( listdate[0], listdate[1], listdate[2] )
                    print("date(a,b,c) is", date) #i.e.: <class 'datetime.date'>
                    print("date( listdate[0], listdate[1], listdate[2] ) is", date( listdate[0], listdate[1], listdate[2] )) #e.g.: 2018-02-13
                    dformat = '%Y'
                    #The base case (where only the year is specified) is set

                    # only month is specified
                    if len( dateparts ) == 2:
                        dformat = '%Y-%m'
                    elif len( dateparts ) == 3 and (dateparts[1] != 1 or (dateparts[1] == 1 and dateparts[2] != 1)):
                        #Checks that the month value is not set to 1, or that, in the case it is, that the day value
                        #is not 1 too (? ask for clarification)
                        dformat = '%Y-%m-%d'

                    date_in_str = date_val.strftime( dformat )
                    print("date_in_str TYPE is a ...", type(date_in_str)) #date_in_str TYPE is a ... <class 'str'>
                    return date_in_str
            except:
                pass
    #in case "issued" field is not present, we try to obtain the year from another field
    elif 'year' in obj:  # Reference object
        ref_year = sub( "[^\d]", "", obj["year"] )[:4]
        if ref_year:
            #note that in this case we already have a string (no need to turn a numerical value into a string)
            print("The TYPE of ref_year is....", type(ref_year))
            return ref_year
    #in case none of the attempts gave a result
    return None

"""
Function aimed at extracting all the needed files for the input directory
"""
def get_all_files(i_dir):
    print("i_dir is:", i_dir)#index/test_data/crossref_dump
    result = []
    opener = None

    # Handles the case in which the files where the data are stored are contained into a zip file
    if i_dir.endswith( ".zip" ):
        zf = ZipFile( i_dir )
        #Once the zip directory is open as such, we iterate all the contained files, identified with .namelist
        for name in zf.namelist():
            #In the case the current file is a Json it is appended to the file list
            if name.lower().endswith( ".json" ):
                result.append( name )
        opener = zf.open
        print ("ZIP FILE OPENER", opener)

    # Handles the case in which the files where the data are stored are contained into a tar file
    elif i_dir.endswith( ".tar.gz" ):
        tf = TarFile.open( i_dir )
        for name in tf.getnames():
            if name.lower().endswith( ".json" ):
                result.append( name )
        opener = tf.extractfile
        print ("TAR FILE OPENER", opener) #what does an opener return?

    # Handles the case in which the files where the data are stored are contained into a normal folder
    else:
        for cur_dir, cur_subdir, cur_files in walk( i_dir ):
            print("cur_dir:", cur_dir, "cur_subdir:" , cur_subdir, "cur_files:",  cur_files, "walk(i_dir);", walk(i_dir))
            #cur_dir: index/test_data/crossref_dump
            #cur_subdir: []
            #cur_files: ['1.json', '2.json', 'citations.csv', 'id_date.csv', 'id_issn.csv', 'id_orcid.csv', 'valid_doi.csv']
            #walk(i_dir); <generator object walk at 0x000001C6616570A0>

            for file in cur_files:
                if file.lower().endswith( '.json' ):
                    #reconstruct the needed format for the filepath to be added to the result list
                    print("For the file", file, "the path is", cur_dir + sep + file)
                    #For the file 1.json the path is index/test_data/crossref_dump\1.json
                    result.append( cur_dir + sep + file) #(What if the file is in a subfolder?)
        opener = open
        print("The OPENER is:", opener) #<built-in function open>
        #result is the list of all the files retrieved from the source
    print("it is returning:", result, opener) # ['index/test_data/crossref_dump\\1.json', 'index/test_data/crossref_dump\\2.json'] <built-in function open>
    return result, opener


def process(input_dir, output_dir):
    #In case the output dir does not exist, it is created
    if not exists( output_dir ):
        makedirs( output_dir )

    #creation of an empty set for the citing Ids which do not have a date
    citing_doi_with_no_date = set()

    #storing in a variable the CSVManager management of the output cvs files
    valid_doi = CSVManager( output_dir + sep + "valid_doi.csv" )
    id_date = CSVManager( output_dir + sep + "id_date_doi.csv" )
    id_issn = CSVManager( output_dir + sep + "id_issn_doi.csv" )
    id_orcid = CSVManager( output_dir + sep + "id_orcid_doi.csv" )

    #preparation of the identifier managers needed
    doi_manager = DOIManager(valid_doi)
    issn_manager = ISSNManager()
    orcid_manager = ORCIDManager()

    # call to the function aimed at retrieve the appropriate opener for
    # the specified input directory and the all the source files contained in the input directory
    all_files, opener = get_all_files(input_dir)

    #length of the list of all source files
    len_all_files = len(all_files)


    # Read all the JSON file in the Crossref dump to create the main information of all the indexes
    print( "\n\n# Add valid DOIs from Crossref metadata" )
    for file_idx, file in enumerate( all_files, 1 ):
        print("these are file_idx and file:", file_idx, "and", file) # e.g.: 1 and index/test_data/crossref_dump\1.json
        print("this is the all_files enumerate obj", enumerate( all_files, 1 )) #e.g.: enumerate obj <enumerate object at 0x0000019D76993EA0>
        with opener( file ) as f:
            print("EFFE",f) # e.g.: <_io.TextIOWrapper name='index/test_data/crossref_dump\\1.json' mode='r' encoding='cp1252'>
            print( "Open file %s of %s" % (file_idx, len_all_files) ) #e.g.: Open file 1 of 2
            try:
                data = load(f)
                print("Type of data is:", type(data),"success of data = load(f). data is:", data)
                #type of data: <class 'dict'>
                #This is the content of the json files used as input

            # When using tar.gz file or zip file a stream of byte is returned by the opener. Thus,
            # it must be converted into an utf-8 string before loading it into a JSON.
            except TypeError:
                utf8reader = codecs.getreader( "utf-8" )
                data = load(utf8reader(f))

            if "items" in data: #main field of the json, whose value is a list of dictionaries, each of which representing a publication
                for obj in data['items']: #for each publication of the list
                    if "DOI" in obj: #if "DOI" is a key in the dictionary representing the publication
                        #the DOI is normalised through its Manager
                        citing_doi = doi_manager.normalise( obj["DOI"], True ) #is it necessary?
                        #note that the normalization step is already included in the set valid.

                        """
                            def normalise(self, id_string, include_prefix=False):
                                try:
                                    doi_string = sub("\0+", "", sub("\s+", "", unquote(id_string[id_string.index("10."):])))
                                    return "%s%s" % (self.p if include_prefix else "", doi_string.lower().strip())
                                except:  # Any error in processing the DOI will return None
                                    return None
                        """

                        #the normalised doi is then set valid
                        doi_manager.set_valid(citing_doi)
                        """
                            def set_valid(self, id_string):
                                doi = self.normalise(id_string, include_prefix=True)
                                if self.valid_doi.get_value(doi) is None:
                                    self.valid_doi.add_value(doi, "v")
                        """

                        if id_date.get_value( citing_doi ) is None:
                            """get_value returns the set of values associated to the input 'id_string',
                            or None if 'id_string' is not included in the CSV.
                            
                                def get_value(self, id_string):
                                    if id_string in self.data:
                                        return set(self.data[id_string])
                            """
                            citing_date = Citation.check_date(build_pubdate(obj))
                            """
                                @staticmethod
                                def check_date(s):
                                    date = sub("\s+", "", s)[:10] if s is not None else ""
                                    if not match("^[0-9]{4}(-[0-9]{2}(-[0-9]{2})?)?$", date):
                                        date = None
                                    if date is not None:
                                        try:  # Check if the date found is valid
                                            parse(date, default=DEFAULT_DATE)
                                        except ValueError:
                                            date = None
                                    return date
                                    """
                            if citing_date is not None:
                                #add the information to the id_date csv file, whose fields are "id" and "value"
                                id_date.add_value( citing_doi, citing_date )
                                #checking if the considered identifier was already inclded in the set for the id without a date
                                if citing_doi in citing_doi_with_no_date:
                                    citing_doi_with_no_date.remove( citing_doi )
                            #If instead the date is absent, the id is added to the set of ids without a date
                            else:
                                citing_doi_with_no_date.add( citing_doi )

                        if id_issn.get_value( citing_doi ) is None: #in order to find the ISSN information
                            if "type" in obj: #check in the "type" field
                                cur_type = obj["type"] #try to understand if the type of the publication
                                #if the type is "journal" and the dictionary of the publication has a "ISSN" field
                                if cur_type is not None and "journal" in cur_type and "ISSN" in obj:
                                    #we store the issn information in a variable
                                    cur_issn = obj["ISSN"]
                                    #we check that the "ISSN" field, although present, is not empty
                                    if cur_issn is not None:
                                        print("[issn_manager.normalise( issn ) for issn in cur_issn]:", [issn_manager.normalise( issn ) for issn in cur_issn])
                                        for issn in [issn_manager.normalise( issn ) for issn in cur_issn]:
                                            #[issn_manager.normalise( issn ) for issn in cur_issn] is a list containing all the appearing ISSNs
                                            #if the issn is not none, it is added to the csv id_issn file
                                            if issn is not None:
                                                id_issn.add_value( citing_doi, issn )

                        #The same procedure is repeated for the orcid
                        if id_orcid.get_value( citing_doi ) is None:
                            #identification of the general field where to find the ORCID
                            if "author" in obj:
                                cur_author = obj['author']
                                #check that the content of the key "author" is not None
                                if cur_author is not None:
                                    print("cur_author type:", type(cur_author))
                                    #iteration for each element contained in the list cur_author
                                    #the elements contained in the list are dicts representing each author
                                    for author in cur_author:
                                        if "ORCID" in author:
                                            #in case the "ORCID" field is present in the dictionary of the author,
                                            #it is then normalised
                                            orcid = orcid_manager.normalise( author["ORCID"] )
                                            if orcid is not None:
                                                #the retrieved data is added to the csv file for the orcid
                                                id_orcid.add_value( citing_doi, orcid )

    # Do it again for updating the dates of the cited DOIs, if these are valid
    print( "\n\n# Check cited DOIs from Crossref reference field" )
    doi_date = {} #creation of an empty set
    for file_idx, file in enumerate( all_files, 1 ):
        with opener( file ) as f:
            print( "Open file %s of %s" % (file_idx, len_all_files) )
            #No try/except step needed
            data = load( f )
            if "items" in data:
                for obj in data['items']:
                    if "DOI" in obj and "reference" in obj: #list of dicts representing the cited publications
                        for ref in obj['reference']:
                            if "DOI" in ref: #identification of the cited doi
                                cited_doi = doi_manager.normalise( ref["DOI"], True ) #normalization of the cited ids
                                if doi_manager.is_valid( cited_doi ) and id_date.get_value( cited_doi ) is None:
                                    # doi_manager.is_valid( cited_doi ) should be True
                                    # id_date.get_value( cited_doi ) is None should be None
                                    if cited_doi not in doi_date: #in the case the file with the dates does not contain the dictionary
                                        doi_date[cited_doi] = []
                                    cited_date = Citation.check_date( build_pubdate( ref ) )
                                    if cited_date is not None:
                                        doi_date[cited_doi].append( cited_date )
                                        if cited_doi in citing_doi_with_no_date:
                                            citing_doi_with_no_date.remove( cited_doi )

    # Add the date to the DOI if such date is the most adopted one in the various references.
    # In case two distinct dates are used the most, select the older one.
    for doi in doi_date:
        count = Counter( doi_date[doi] )
        if len( count ):
            top_value = count.most_common( 1 )[0][1]
            selected_dates = []
            for date in count:
                if count[date] == top_value:
                    selected_dates.append( date )
            best_date = sorted( selected_dates )[0]
            id_date.add_value( doi, best_date )
        else:
            id_date.add_value( doi, "" )

    # Add emtpy dates for the remaining DOIs
    for doi in citing_doi_with_no_date:
        id_date.add_value( doi, "" )


if __name__ == "__main__":
    arg_parser = ArgumentParser( "Global files creator for COCI",
                                 description="Process Crossref JSON files and create global indexes to enable "
                                             "the creation of COCI." )
    arg_parser.add_argument( "-i", "--input_dir", dest="input_dir", required=True,
                             help="Either the directory or the zip file that contains the Crossref data dump "
                                  "of JSON files." )
    arg_parser.add_argument( "-o", "--output_dir", dest="output_dir", required=True,
                             help="The directory where the indexes are stored." )

    args = arg_parser.parse_args()
    process( args.input_dir, args.output_dir )

#How to call the service for COCI
#python -m index.coci.glob -i "index/test_data/crossref_dump" -o "index/test_data/crossref_glob"

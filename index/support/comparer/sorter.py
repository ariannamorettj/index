from zipfile import ZipFile
from tarfile import TarFile
from os import sep, makedirs, walk
from argparse import ArgumentParser
from os.path import exists
from io import StringIO
from rdflib import Graph, URIRef
import rdflib

#importa i files
# Function aimed at extracting all the needed files for the input directory
def get_all_files(i_dir):
    result = []
    opener = None

    if i_dir.endswith( ".zip" ):
        zf = ZipFile( i_dir )
        for name in zf.namelist():
            result.append( name )
        opener = zf.open
    elif i_dir.endswith( ".tar.gz" ):
        tf = TarFile.open( i_dir )
        for name in tf.getnames():
            result.append(name)
        opener = tf.extractfile

    else:
        for cur_dir, cur_subdir, cur_files in walk(i_dir):
            for file in cur_files:
                result.append(cur_dir + sep + file)
        opener = open
    return result, opener

def process(input_dir, output_dir):

    #1) Open all files and create the output directory if it does exist
    if not exists(output_dir):
        makedirs(output_dir)

    all_files, opener = get_all_files(input_dir)
    print("allfiles:", all_files)
    print("qui 1")
    for file_idx, file in enumerate( all_files, 1 ):
        print( "qui 2" )
        with open(file, 'r' ) as f:
            data = f.read()
        print("data:", data)

        #2) divide the file content line by line

        lines = data.split("\n")
        print("lines:", lines)
        non_empty_lines = [line for line in lines if line.strip() != ""]
        print("non empty lines:", non_empty_lines)

        #3) sort them lexographically
        non_empty_lines.sort()

        #4) create a unique string
        string_without_empty_lines = ""
        for line in non_empty_lines:
            string_without_empty_lines += (line + "\n")
        print("string_without_empty_lines", string_without_empty_lines)

        #5) write the content to a text file
        with open(output_dir + sep + str(file_idx) +".txt", 'w' ) as f:
            f.write(string_without_empty_lines)


if __name__ == "__main__":
    arg_parser = ArgumentParser( "File to make files internally uniform, so to compare them",
                                 description="Takes a directory containing files in input and creates textual versions "
                                             "of the same files, so to compare them" )
    arg_parser.add_argument( "-i", "--input_dir", dest="input_dir", required=True,
                             help="Either the directory or the zip file that contains the NIH data dump "
                                  "of CSV files." )
    arg_parser.add_argument( "-o", "--output_dir", dest="output_dir", required=True,
                             help="The directory where the output files are stored." )


    args = arg_parser.parse_args()
    process(args.input_dir, args.output_dir)


#python -m index.support.comparer.sorter -i "index/support/comparer/testfiles" -o "index/support/comparer/outputfiles"
from zipfile import ZipFile
from tarfile import TarFile
from os import sep, makedirs, walk
from argparse import ArgumentParser

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

def process(input_dir):
    all_files, opener = get_all_files(input_dir)
    for file_idx, file in enumerate( all_files, 1 ):
        with open(file, 'r' ) as f:
            data = f.read()
        for file_idx_tc, file_to_compare in enumerate( all_files, 1 ):
            if file != file_to_compare:
                with open( file_to_compare, 'r' ) as f_tc:
                    data_tc = f_tc.read()
                if data == data_tc:
                    print("The content of file n.", file_idx, "IS THE SAME AS the content of file n.", file_idx_tc)
                else:
                    print("The content of file n.", file_idx, "DIFFERS from the content of file n.", file_idx_tc)
                    data_split = set(data.splitlines())
                    data_tc_split = set(data_tc.splitlines())
                    print("Set difference: ", data_split.difference(data_tc_split))
                    print("Set difference: ", data_tc_split.difference(data_split))



if __name__ == "__main__":
    arg_parser = ArgumentParser( "File to check whether the textual content of two or more files is the same or not",
                                 description="Takes a directory containing files in input and creates textual versions "
                                             "of the same files, so to compare them" )
    arg_parser.add_argument( "-i", "--input_dir", dest="input_dir", required=True,
                             help="directory containing the files to be compared" )

    args = arg_parser.parse_args()
    process(args.input_dir)


#python -m index.support.comparer.comparer -i "index/support/comparer/outputfiles"
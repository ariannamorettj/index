#qui una classe che estende mapping (classe) in modo da potergli passare l'input in più che estende la classe e che mi va a generare
#il file di mapping.
#l'identificativo va passato come input per capire quali files di mapping considerare e quali no
#per la selezione o l'utente passa i files di mapping (che devono essere fatti in un certo modo) o li selezioni con il nome.
#il file di mapping crealo come vuoi, è ragionevole utilizzare csv manager, l'importante è occupare poco spazio. forse la cosa più
#safe è che ci fosse un prefisso per ogni riga, così da mantenere la struttura coerente per tutti i files.
#passa in input anche il tipo
#in input serve anche il numero dell'utimo metaid creato
#fai in modo di generare il rdf solo alla fine, prendendo nota inizialmente del numero di righe del csv in input per capire da quale riga in poi creare l'rdf
#i file di mapping puoi considerare solo quelli che hanni i prefissi che ti interessano
#è importante che tutti i file di mapping contengano l'informazione del tipo di id.

import pandas as pd
from argparse import ArgumentParser
from index.storer.csvmanager import CSVManager
from index.identifier.pmidmanager import PMIDManager
from index.identifier.doimanager import DOIManager
from os import sep, makedirs, walk
from os.path import exists
from zipfile import ZipFile
from tarfile import TarFile

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


def process(input_dir, output_dir):
    if not exists(output_dir):
        makedirs(output_dir)

    mapping_folder = output_dir + "/mapping"
    if not exists(mapping_folder):
        makedirs(mapping_folder)

    pmid_doi_mapping = CSVManager(mapping_folder + sep + "pmid_doi_mapping.csv" )
    valid_pmid = CSVManager(output_dir + sep + "valid_pmid.csv")
    valid_doi = CSVManager("index/test_data/crossref_glob" + sep + "valid_doi.csv")
    pmid_manager = PMIDManager(valid_pmid)
    doi_manager = DOIManager(valid_doi)
    all_files, opener = get_all_files(input_dir)
    len_all_files = len(all_files)

    # Read all the CSV file in the NIH dump to create the main information of all the indexes
    print( "\n\n# Add valid PMIDs from NIH metadata" )
    for file_idx, file in enumerate( all_files, 1 ):
        df = pd.DataFrame()
        for chunk in pd.read_csv(file, chunksize=1000 ):
            f = pd.concat( [df, chunk], ignore_index=True )
            print( "Open file %s of %s" % (file_idx, len_all_files) )

            for index, row in f.iterrows():
                print(index)
                pmid = str(row["pmid"])
                doi = str(row["doi"])
                if pmid != "" and doi != "":
                    if pmid_manager.is_valid(pmid) and doi_manager.is_valid(doi):
                        pmid = pmid_manager.p + pmid
                        doi = doi_manager.p + doi
                        pmid_doi_mapping.add_value(pmid, doi)

if __name__ == "__main__":
    arg_parser = ArgumentParser( "Global files creator for mapping PMID-DOI",
                                 description="Process NIH CSV files and create a mapping with DOIs, where the "
                                             "information is provided.")
    arg_parser.add_argument( "-i", "--input_dir", dest="input_dir", required=True,
                             help="Either the directory or the zip file that contains the NIH data dump "
                                  "of CSV files." )
    arg_parser.add_argument( "-o", "--output_dir", dest="output_dir", required=True,
                             help="The directory where the mapping is stored." )

    args = arg_parser.parse_args()
    process(args.input_dir, args.output_dir)


#python -m index.noci.nocimapping -i "index/test_data/nih_dump" -o "index/test_data/nih_glob1"



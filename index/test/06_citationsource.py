#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2019, Silvio Peroni <essepuntato@gmail.com>
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

import unittest
from os import sep, remove
from os.path import exists
from index.citation.citationsource import CSVFileCitationSource
from index.citation.oci import Citation, OCIManager
from urllib.parse import quote
from index.storer.citationstorer import CitationStorer


class CitationSourceTest(unittest.TestCase):
    """This class aim at testing the methods of the class CSVManager."""

    def setUp(self):
        info_file_path = "index%stest_data%stmp_store%sdata%s.dir_citation_source" % (sep, sep, sep, sep)
        if exists(info_file_path):
            remove(info_file_path)
        info_file_path_pmid = "index%stest_data%stmp_store_pmid%sdata%s.dir_citation_source" % (sep, sep, sep, sep)
        if exists(info_file_path_pmid):
            remove(info_file_path_pmid)
        self.oci = OCIManager(lookup_file="index%stest_data%slookup_full.csv" % (sep, sep))
        self.citation_list = CitationStorer.load_citations_from_file(
            "index%stest_data%scitations_data.csv" % (sep, sep),
            "index%stest_data%scitations_prov.csv" % (sep, sep),
            baseurl="http://dx.doi.org/",
            service_name="OpenCitations Index: COCI", id_type="doi",
            id_shape="http://dx.doi.org/([[XXX__decode]])", citation_type=None)
        #quale id shape per pmid?
        self.citation_list_pmid = CitationStorer.load_citations_from_file(
            "index%stest_data%scitations_pmid_data.csv" % (sep, sep),
            "index%stest_data%scitations_pmid_prov.csv" % (sep, sep),
            baseurl="https://pubmed.ncbi.nlm.nih.gov/",
            service_name="OpenCitations Index: NOCI", id_type="pmid",
            id_shape="https://pubmed.ncbi.nlm.nih.gov/([[XXX__decode]])", citation_type=None)

    def __create_citation(self, citing, cited, created, timespan, journal_sc, author_sc, id_type):
        if id_type == OCIManager.doi_type:
            return Citation(
                self.oci.get_oci(citing, cited, "020"),
                "http://dx.doi.org/" + quote(citing), None,
                "http://dx.doi.org/" + quote(cited), None,
                created, timespan,
                1, "https://w3id.org/oc/index/prov/ra/1",
                "https://api.crossref.org/works/" + quote(citing), "2018-01-01T00:00:00",
                "OpenCitations Index: COCI", "doi", "http://dx.doi.org/([[XXX__decode]])", None,
                journal_sc, author_sc, prov_description="Creation of the citation")
        elif id_type == OCIManager.pmid_type: #RICONTROLLARE QUESTA PARTE CON DATI NECESSARI PER I TEST
            #dubbi su: "https://doi.org/10.35092/yhjc.c.4586573.v16" + quote(citing) : così non esce chiaramente niente perché la mia risorsa è un file, come faccio?
            #dubbi su: cosa sia il corrispettivo dell'id_shape per pmid, ovvero di "http://dx.doi.org/([[XXX__decode]])"
            return Citation(
                self.oci.get_oci(citing, cited, "0160"),
                "https://pubmed.ncbi.nlm.nih.gov/" + quote(citing), None,
                "https://pubmed.ncbi.nlm.nih.gov/" + quote(cited), None,
                created, timespan,
                1, "https://w3id.org/oc/index/prov/ra/1",
                "https://doi.org/10.35092/yhjc.c.4586573.v16", "2018-01-01T00:00:00",
                "OpenCitations Index: NOCI", "pmid", "https://pubmed.ncbi.nlm.nih.gov/([[XXX__decode]])", None,
                journal_sc, author_sc, prov_description="Creation of the citation")
        else:
            print("the specified id_type is not compliant")

    def __citations_csv(self, origin_citation_list, stored_citation_list):
        l1 = [cit.get_citation_csv() for cit in origin_citation_list]
        l2 = [cit.get_citation_csv() for cit in stored_citation_list]
        self.assertEqual(len(l1), len(l2))
        self.assertEqual(set(l1), set(l2))

    def test_get_next_citation_data(self):
        cs = CSVFileCitationSource("index%stest_data%stmp_store%sdata" % (sep, sep, sep))
        citation_1 = self.__create_citation(*cs.get_next_citation_data(), "doi")
        citation_2 = self.__create_citation(*cs.get_next_citation_data(), "doi")
        self.__citations_csv(self.citation_list[:2], [citation_1, citation_2])
        cs = CSVFileCitationSource( "index%stest_data%stmp_store%sdata" % (sep, sep, sep) )
        citation_3 = self.__create_citation( *cs.get_next_citation_data(), "doi" )
        citation_4 = self.__create_citation( *cs.get_next_citation_data(), "doi" )
        citation_5 = self.__create_citation( *cs.get_next_citation_data(), "doi" )
        citation_6 = self.__create_citation( *cs.get_next_citation_data(), "doi" )
        self.__citations_csv( self.citation_list[2:6], [citation_3, citation_4, citation_5, citation_6] )

        idx = 0
        while cs.get_next_citation_data() is not None:
            idx += 1
        self.assertEqual( idx, 6 )

    def test_get_next_citation_data_pmid(self):
        cs = CSVFileCitationSource("index%stest_data%stmp_pmid_store%sdata" % (sep, sep, sep))
        citation_1 = self.__create_citation(*cs.get_next_citation_data(), "pmid")
        citation_2 = self.__create_citation(*cs.get_next_citation_data(), "pmid")
        self.__citations_csv(self.citation_list_pmid[:2], [citation_1, citation_2])
        cs = CSVFileCitationSource("index%stest_data%stmp_pmid_store%sdata" % (sep, sep, sep) )
        citation_3 = self.__create_citation( *cs.get_next_citation_data(), "pmid" )
        citation_4 = self.__create_citation( *cs.get_next_citation_data(), "pmid" )
        citation_5 = self.__create_citation( *cs.get_next_citation_data(), "pmid" )
        citation_6 = self.__create_citation( *cs.get_next_citation_data(), "pmid" )
        self.__citations_csv( self.citation_list_pmid[2:6], [citation_3, citation_4, citation_5, citation_6] )

        idx = 0
        while cs.get_next_citation_data() is not None:
            idx += 1
        self.assertEqual( idx, 6 )



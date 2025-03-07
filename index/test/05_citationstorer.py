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
from index.storer.citationstorer import CitationStorer
from os import sep
from shutil import rmtree
from os import walk
from os.path import exists
from rdflib.compare import isomorphic, graph_diff
from rdflib import Graph, ConjunctiveGraph
from rdflib.namespace import XSD
from rdflib.term import _toPythonMapping
from glob import glob
from pprint import pprint


class CitationStorerTest(unittest.TestCase):
    """This class aim at testing the methods of the class CitationStorer."""

    def setUp(self):
        self.citation_data_csv_path = "index%stest_data%scitations_data.csv" % (sep, sep)
        self.citation_prov_csv_path = "index%stest_data%scitations_prov.csv" % (sep, sep)
        self.citation_data_ttl_path = "index%stest_data%scitations_data.ttl" % (sep, sep)
        self.citation_prov_ttl_path = "index%stest_data%scitations_prov.ttl" % (sep, sep)
        self.citation_data_prov_scholix_path = "index%stest_data%scitations_data_prov.scholix" % (sep, sep)
        self.tmp_path = "index%stest_data%stmp" % (sep, sep)
        self.baseurl = "https://w3id.org/oc/index/coci/"

        self.citation_data_csv_path_pmid = "index%stest_data%scitations_pmid_data.csv" % (sep, sep)
        self.citation_prov_csv_path_pmid = "index%stest_data%scitations_pmid_prov.csv" % (sep, sep)
        self.citation_data_ttl_path_pmid = "index%stest_data%scitations_pmid_data.ttl" % (sep, sep)
        self.citation_prov_ttl_path_pmid = "index%stest_data%scitations_pmid_prov.ttl" % (sep, sep)
        self.citation_data_prov_scholix_path_pmid = "index%stest_data%scitations_pmid_data_prov.scholix" % (sep, sep)
        self.tmp_path_pmid = "index%stest_data%stmp_pmid" % (sep, sep)
        self.baseurl_pmid = "https://w3id.org/oc/index/noci/"

        self.ext_local_dir = {
            "ttl": "rdf",
            "scholix": "slx",
            "csv": "csv"
        }

        # Hack for correct handling of date datatypes
        if XSD.gYear in _toPythonMapping:
            _toPythonMapping.pop(XSD.gYear)
        if XSD.gYearMonth in _toPythonMapping:
            _toPythonMapping.pop(XSD.gYearMonth)

    def load_and_store_citations(self, data_path, prov_path, ext):
        tmp_path = self.tmp_path + "_load"

        if exists(tmp_path):
            rmtree(tmp_path)

        origin_citation_list = CitationStorer.load_citations_from_file(
            data_path, prov_path, baseurl="http://dx.doi.org/",
            service_name="OpenCitations Index: COCI", id_type="doi",
            id_shape="http://dx.doi.org/([[XXX__decode]])", citation_type=None)

        cs = CitationStorer(tmp_path, self.baseurl)
        for citation in origin_citation_list:
            cs.store_citation(citation)

        stored_citation_list = CitationStorerTest.get_stored_citation_list(
            tmp_path + sep + "data" + sep + self.ext_local_dir[ext] + sep, ext)

        return origin_citation_list, stored_citation_list

    def load_and_store_citations_pmid(self, data_path, prov_path, ext):
        tmp_path = self.tmp_path_pmid + "_load"

        if exists(tmp_path):
            rmtree(tmp_path)

        origin_citation_list = CitationStorer.load_citations_from_file(
            data_path, prov_path, baseurl="https://pubmed.ncbi.nlm.nih.gov/",
            service_name="OpenCitations Index: NOCI", id_type="pmid",
            id_shape="https://pubmed.ncbi.nlm.nih.gov/([[XXX__decode]])", citation_type=None)

        cs = CitationStorer(tmp_path, self.baseurl_pmid)
        for citation in origin_citation_list:
            cs.store_citation(citation)

        stored_citation_list = CitationStorerTest.get_stored_citation_list_pmid(
            tmp_path + sep + "data" + sep + self.ext_local_dir[ext] + sep, ext)

        return origin_citation_list, stored_citation_list

    def test_load_citations_csv(self):
        origin_citation_list, stored_citation_list = self.load_and_store_citations(
            self.citation_data_csv_path, self.citation_prov_csv_path, "csv")

        self.citations_csv(origin_citation_list, stored_citation_list)

    def test_load_citations_csv_pmid(self):
        origin_citation_list, stored_citation_list = self.load_and_store_citations_pmid(
            self.citation_data_csv_path_pmid, self.citation_prov_csv_path_pmid, "csv")

        self.citations_csv(origin_citation_list, stored_citation_list)

    def citations_csv(self, origin_citation_list, stored_citation_list):
        l1 = [cit.get_citation_csv() for cit in origin_citation_list]
        l2 = [cit.get_citation_csv() for cit in stored_citation_list]
        self.assertEqual(len(l1), len(l2))
        self.assertEqual(set(l1), set(l2))

    def test_load_citations_rdf(self):
        origin_citation_list, stored_citation_list = self.load_and_store_citations(
            self.citation_data_ttl_path, self.citation_prov_ttl_path, "ttl")

        self.citations_rdf(origin_citation_list, stored_citation_list)

    def test_load_citations_rdf_pmid(self):
        origin_citation_list, stored_citation_list = self.load_and_store_citations_pmid(
            self.citation_data_ttl_path_pmid, self.citation_prov_ttl_path_pmid, "ttl")

        self.citations_rdf(origin_citation_list, stored_citation_list)

    def citations_rdf(self, origin_citation_list, stored_citation_list):
        g1 = ConjunctiveGraph()
        g2 = ConjunctiveGraph()

        for idx, cit in enumerate(origin_citation_list):
            for s, p, o, g in cit.get_citation_rdf(
                    self.baseurl, False, False, True).quads((None, None, None, None)):
                g1.add((s, p, o, g))
            for s, p, o, g in stored_citation_list[idx].get_citation_rdf(
                    self.baseurl, False, False, True).quads((None, None, None, None)):
                g2.add((s, p, o, g))
        
        s1 = "\n".join(sorted(g1.serialize(
            format="nt11", encoding="utf-8").decode("utf-8").split("\n")))
        s2 = "\n".join(sorted(g1.serialize(
            format="nt11", encoding="utf-8").decode("utf-8").split("\n")))

        self.assertEqual(s1, s2)

    def citations_rdf_pmid(self, origin_citation_list, stored_citation_list):
        g1 = ConjunctiveGraph()
        g2 = ConjunctiveGraph()

        for idx, cit in enumerate(origin_citation_list):
            for s, p, o, g in cit.get_citation_rdf(
                    self.baseurl_pmid, False, False, True).quads((None, None, None, None)):
                g1.add((s, p, o, g))
            for s, p, o, g in stored_citation_list[idx].get_citation_rdf(
                    self.baseurl_pmid, False, False, True).quads((None, None, None, None)):
                g2.add((s, p, o, g))

        s1 = "\n".join(sorted(g1.serialize(
            format="nt11", encoding="utf-8").decode("utf-8").split("\n")))
        s2 = "\n".join(sorted(g1.serialize(
            format="nt11", encoding="utf-8").decode("utf-8").split("\n")))

        self.assertEqual(s1, s2)

    def test_load_citations_slx(self):
        origin_citation_list, stored_citation_list = self.load_and_store_citations(
            self.citation_data_prov_scholix_path, None, "scholix")

        self.citations_slx(origin_citation_list, stored_citation_list)

    def test_load_citations_slx_pmid(self):
        origin_citation_list, stored_citation_list = self.load_and_store_citations_pmid(
            self.citation_data_prov_scholix_path_pmid, None, "scholix")

        self.citations_slx(origin_citation_list, stored_citation_list)

    def citations_slx(self, origin_citation_list, stored_citation_list):
        l1 = [cit.get_citation_scholix() for cit in origin_citation_list]
        l2 = [cit.get_citation_scholix() for cit in stored_citation_list]
        self.assertEqual(len(l1), len(l2))
        self.assertEqual(set(l1), set(l2))

    def test_store_citation(self):
        tmp_subpath = "_store"
        tmp_path = self.tmp_path + tmp_subpath

        if exists(tmp_path):
            rmtree(tmp_path)

        origin_citation_list = CitationStorer.load_citations_from_file(
            self.citation_data_csv_path, self.citation_prov_csv_path, baseurl="http://dx.doi.org/",
            service_name="OpenCitations Index: COCI", id_type="doi",
            id_shape="http://dx.doi.org/([[XXX__decode]])", citation_type=None)

        cs = CitationStorer(tmp_path, self.baseurl,
                            n_citations_csv_file=4, n_citations_rdf_file=2, n_citations_slx_file=3)
        for citation in origin_citation_list:
            cs.store_citation(citation)

        data_path = tmp_path + sep + "data"
        csv_data_path = data_path + sep + "csv" + sep
        rdf_data_path = data_path + sep + "rdf" + sep
        slx_data_path = data_path + sep + "slx" + sep

        prov_path = tmp_path + sep + "prov" + sep
        csv_prov_path = prov_path + sep + "csv" + sep
        rdf_prov_path = prov_path + sep + "rdf" + sep

        # Check if directories exist
        self.assertTrue(all([exists(p) for p in
                             [csv_data_path, rdf_data_path, slx_data_path, csv_prov_path, rdf_prov_path]]))

        # Check if files exist
        self.assertEqual(len([f for f in glob(csv_data_path + "**" + sep + "*.csv", recursive=True)]), 2)
        self.assertEqual(len([f for f in glob(csv_prov_path + "**" + sep + "*.csv", recursive=True)]), 2)
        self.assertEqual(len([f for f in glob(rdf_data_path + "**" + sep + "*.ttl", recursive=True)]), 3)
        self.assertEqual(len([f for f in glob(rdf_prov_path + "**" + sep + "*.ttl", recursive=True)]), 3)
        self.assertEqual(len([f for f in glob(slx_data_path + "**" + sep + "*.scholix", recursive=True)]), 2)

        # Check if the new stored files contains the same citations of the original one
        stored_citation_list_csv = CitationStorerTest.get_stored_citation_list(csv_data_path, "csv")
        self.citations_csv(origin_citation_list, stored_citation_list_csv)
        stored_citation_list_rdf = CitationStorerTest.get_stored_citation_list(rdf_data_path, "ttl")
        self.citations_rdf(origin_citation_list, stored_citation_list_rdf)
        stored_citation_list_slx = CitationStorerTest.get_stored_citation_list(slx_data_path, "scholix")
        self.citations_slx(origin_citation_list, stored_citation_list_slx)

        # Store again all citations previously stored and checked in they are correctly
        # added to the existing files
        for citation in origin_citation_list:
            cs.store_citation(citation)
        self.assertEqual(len([f for f in glob(csv_data_path + "**" + sep + "*.csv", recursive=True)]), 3)
        self.assertEqual(len([f for f in glob(csv_prov_path + "**" + sep + "*.csv", recursive=True)]), 3)
        self.assertEqual(len([f for f in glob(rdf_data_path + "**" + sep + "*.ttl", recursive=True)]), 6)
        self.assertEqual(len([f for f in glob(rdf_prov_path + "**" + sep + "*.ttl", recursive=True)]), 6)
        self.assertEqual(len([f for f in glob(slx_data_path + "**" + sep + "*.scholix", recursive=True)]), 4)


    def test_store_citation_pmid(self):
        tmp_subpath = "_store"
        tmp_path = self.tmp_path_pmid + tmp_subpath

        if exists(tmp_path):
            rmtree(tmp_path)

        origin_citation_list = CitationStorer.load_citations_from_file(
            self.citation_data_csv_path_pmid, self.citation_prov_csv_path_pmid, baseurl="https://pubmed.ncbi.nlm.nih.gov/",
            service_name="OpenCitations Index: NOCI", id_type="pmid",
            id_shape="https://pubmed.ncbi.nlm.nih.gov/([[XXX__decode]])", citation_type=None)

        cs = CitationStorer(tmp_path, self.baseurl_pmid,
                            n_citations_csv_file=4, n_citations_rdf_file=2, n_citations_slx_file=3)
        for citation in origin_citation_list:
            cs.store_citation(citation)

        data_path = tmp_path + sep + "data"
        csv_data_path = data_path + sep + "csv" + sep
        rdf_data_path = data_path + sep + "rdf" + sep
        slx_data_path = data_path + sep + "slx" + sep

        prov_path = tmp_path + sep + "prov" + sep
        csv_prov_path = prov_path + sep + "csv" + sep
        rdf_prov_path = prov_path + sep + "rdf" + sep

        # Check if directories exist
        self.assertTrue(all([exists(p) for p in
                             [csv_data_path, rdf_data_path, slx_data_path, csv_prov_path, rdf_prov_path]]))

        # Check if files exist
        self.assertEqual(len([f for f in glob(csv_data_path + "**" + sep + "*.csv", recursive=True)]), 2)
        self.assertEqual(len([f for f in glob(csv_prov_path + "**" + sep + "*.csv", recursive=True)]), 2)
        self.assertEqual(len([f for f in glob(rdf_data_path + "**" + sep + "*.ttl", recursive=True)]), 3)
        self.assertEqual(len([f for f in glob(rdf_prov_path + "**" + sep + "*.ttl", recursive=True)]), 3)
        self.assertEqual(len([f for f in glob(slx_data_path + "**" + sep + "*.scholix", recursive=True)]), 2)

        # Check if the new stored files contains the same citations of the original one
        stored_citation_list_csv = CitationStorerTest.get_stored_citation_list_pmid(csv_data_path, "csv")
        self.citations_csv(origin_citation_list, stored_citation_list_csv)
        stored_citation_list_rdf = CitationStorerTest.get_stored_citation_list_pmid(rdf_data_path, "ttl")
        self.citations_rdf_pmid(origin_citation_list, stored_citation_list_rdf)
        stored_citation_list_slx = CitationStorerTest.get_stored_citation_list_pmid(slx_data_path, "scholix")
        self.citations_slx(origin_citation_list, stored_citation_list_slx)

        # Store again all citations previously stored and checked in they are correctly
        # added to the existing files
        for citation in origin_citation_list:
            cs.store_citation(citation)
        self.assertEqual(len([f for f in glob(csv_data_path + "**" + sep + "*.csv", recursive=True)]), 3)
        self.assertEqual(len([f for f in glob(csv_prov_path + "**" + sep + "*.csv", recursive=True)]), 3)
        self.assertEqual(len([f for f in glob(rdf_data_path + "**" + sep + "*.ttl", recursive=True)]), 6)
        self.assertEqual(len([f for f in glob(rdf_prov_path + "**" + sep + "*.ttl", recursive=True)]), 6)
        self.assertEqual(len([f for f in glob(slx_data_path + "**" + sep + "*.scholix", recursive=True)]), 4)

    @staticmethod
    def get_stored_citation_list(data_path, ext):
        stored_citation_list = []

        for f in [f for f in glob(data_path + "**/*." + ext, recursive=True)]:
            stored_citation_list.extend(CitationStorer.load_citations_from_file(
                f, f.replace("%sdata%s" % (sep, sep), "%sprov%s" % (sep, sep)),
                baseurl="http://dx.doi.org/", service_name="OpenCitations Index: COCI", id_type="doi",
                id_shape="http://dx.doi.org/([[XXX__decode]])", citation_type=None))

        return stored_citation_list\


    @staticmethod
    def get_stored_citation_list_pmid(data_path, ext):
        stored_citation_list = []

        for f in [f for f in glob(data_path + "**/*." + ext, recursive=True)]:
            stored_citation_list.extend(CitationStorer.load_citations_from_file(
                f, f.replace("%sdata%s" % (sep, sep), "%sprov%s" % (sep, sep)),
                baseurl="https://pubmed.ncbi.nlm.nih.gov/", service_name="OpenCitations Index: NOCI", id_type="pmid",
                id_shape="https://pubmed.ncbi.nlm.nih.gov/([[XXX__decode]])", citation_type=None))

        return stored_citation_list

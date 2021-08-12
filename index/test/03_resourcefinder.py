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
from os import sep
from index.storer.csvmanager import CSVManager
from index.finder.crossrefresourcefinder import CrossrefResourceFinder
from index.finder.dataciteresourcefinder import DataCiteResourceFinder
from index.finder.nihresourcefinder import NIHResourceFinder
from index.finder.orcidresourcefinder import ORCIDResourceFinder
from index.finder.resourcefinder import ResourceFinderHandler


class ResourceFinderTest(unittest.TestCase):
    """This class aim at testing the methods of the class CSVManager."""

    def setUp(self):
        self.date_path = "index%stest_data%sid_date.csv" % (sep, sep)
        self.date_path_pmid = "index%stest_data%sid_date_pmid.csv" % (sep, sep)
        self.orcid_path = "index%stest_data%sid_orcid.csv" % (sep, sep)
        self.orcid_path_pmid = "index%stest_data%sid_orcid_pmid.csv" % (sep, sep)
        self.issn_path = "index%stest_data%sid_issn.csv" % (sep, sep)
        self.issn_path_pmid = "index%stest_data%sid_issn_pmid.csv" % (sep, sep)
        self.doi_path = "index%stest_data%svalid_doi.csv" % (sep, sep)
        self.pmid_path = "index%stest_data%svalid_pmid.csv" % (sep, sep)

    def test_handler_get_date(self):
        handler = ResourceFinderHandler(
            [CrossrefResourceFinder(), DataCiteResourceFinder(), ORCIDResourceFinder(), NIHResourceFinder()])
        self.assertEqual("2019-05-27", handler.get_date("10.6092/issn.2532-8816/8555"))
        self.assertNotEqual("2019-05-27", handler.get_date("10.1108/jd-12-2013-0166"))

    def test_handler_share_issn(self):
        handler = ResourceFinderHandler(
            [CrossrefResourceFinder(), DataCiteResourceFinder(), ORCIDResourceFinder(), NIHResourceFinder()])
        self.assertTrue(handler.share_issn("10.1007/s11192-018-2988-z", "10.1007/s11192-015-1565-y"))
        #self.assertTrue(handler.share_issn("1609936", "1509982"))
        self.assertFalse(handler.share_issn("10.1007/s11192-018-2988-z", "10.6092/issn.2532-8816/8555"))

    def test_handler_share_orcid(self):
        handler = ResourceFinderHandler(
            [CrossrefResourceFinder(), DataCiteResourceFinder(), ORCIDResourceFinder(), NIHResourceFinder()])
        self.assertTrue(handler.share_orcid("10.1007/s11192-018-2988-z", "10.5281/zenodo.3344898"))
        self.assertFalse(handler.share_orcid("10.1007/s11192-018-2988-z", "10.1007/s11192-015-1565-y5"))

    def test_orcid_get_orcid(self):
        # Do not use support files, only APIs
        of_1 = ORCIDResourceFinder()
        self.assertIn("0000-0003-0530-4305", of_1.get_orcid("10.1108/jd-12-2013-0166"))
        self.assertNotIn("0000-0001-5506-523X", of_1.get_orcid("10.1108/jd-12-2013-0166"))

        # Do use support files, but avoid using APIs
        of_2 = ORCIDResourceFinder(orcid=CSVManager(self.orcid_path),
                                   doi=CSVManager(self.doi_path), use_api_service=False)
        print("of_2.dm.valid_doi.csv_path", of_2.dm.valid_doi.csv_path)
        container = of_2.get_orcid("10.1108/jd-12-2013-0166")
        print("CONTAINER IS NONE?", container)
        self.assertIn("0000-0003-0530-4305", container)
        self.assertNotIn("0000-0001-5506-523X", of_2.get_orcid("10.1108/jd-12-2013-0166"))

        # Do not use support files neither APIs
        of_3 = ORCIDResourceFinder(use_api_service=False)
        self.assertIsNone(of_3.get_orcid("10.1108/jd-12-2013-0166"))

    def test_datacite_get_orcid(self):
        # Do not use support files, only APIs
        df_1 = DataCiteResourceFinder()
        self.assertIn("0000-0001-7734-8388", df_1.get_orcid("10.5065/d6b8565d"))
        self.assertNotIn("0000-0001-5506-523X", df_1.get_orcid("10.5065/d6b8565d"))

        # Do use support files, but avoid using APIs
        df_2 = DataCiteResourceFinder(orcid=CSVManager(self.orcid_path),
                                      doi=CSVManager(self.doi_path), use_api_service=False)
        self.assertIn("0000-0001-7734-8388", df_2.get_orcid("10.5065/d6b8565d"))
        self.assertNotIn("0000-0001-5506-523X", df_2.get_orcid("10.5065/d6b8565d"))

        # Do not use support files neither APIs
        df_3 = DataCiteResourceFinder(use_api_service=False)
        self.assertIsNone(df_3.get_orcid("10.5065/d6b8565d"))

    def test_datacite_get_issn(self):
        # Do not use support files, only APIs
        df_1 = DataCiteResourceFinder()
        self.assertIn("1406-894X", df_1.get_container_issn("10.15159/ar.21.030"))
        self.assertNotIn("1588-2861", df_1.get_container_issn("10.15159/ar.21.030"))

        # Do use support files, but avoid using APIs
        df_2 = DataCiteResourceFinder(issn=CSVManager(self.issn_path),
                                      doi=CSVManager(self.doi_path), use_api_service=False)
        self.assertIn("2197-6775", df_2.get_container_issn("10.14763/2019.1.1389"))
        self.assertNotIn("1588-2861", df_2.get_container_issn("10.14763/2019.1.1389"))

        # Do not use support files neither APIs
        df_3 = DataCiteResourceFinder(use_api_service=False)
        self.assertIsNone(df_3.get_container_issn("10.14763/2019.1.1389"))

    def test_datacite_get_pub_date(self):
        # Do not use support files, only APIs
        df_1 = DataCiteResourceFinder()
        self.assertIn("2019-05-27", df_1.get_pub_date("10.6092/issn.2532-8816/8555"))
        self.assertNotEqual("2019", df_1.get_pub_date("10.6092/issn.2532-8816/8555"))

        # Do use support files, but avoid using APIs
        df_2 = DataCiteResourceFinder(date=CSVManager(self.date_path),
                                      doi=CSVManager(self.doi_path), use_api_service=False)
        self.assertIn("2019-05-27", df_2.get_pub_date("10.6092/issn.2532-8816/8555"))
        self.assertNotEqual("2018-01-02", df_2.get_pub_date("10.6092/issn.2532-8816/8555"))

        # Do not use support files neither APIs
        df_3 = DataCiteResourceFinder(use_api_service=False)
        self.assertIsNone(df_3.get_pub_date("10.6092/issn.2532-8816/8555"))

    def test_crossref_get_orcid(self):
        # Do not use support files, only APIs
        cf_1 = CrossrefResourceFinder()
        self.assertIn("0000-0003-0530-4305", cf_1.get_orcid("10.1007/s11192-018-2988-z"))
        self.assertNotIn("0000-0001-5506-523X", cf_1.get_orcid("10.1007/s11192-018-2988-z"))

        # Do use support files, but avoid using APIs
        cf_2 = CrossrefResourceFinder(orcid=CSVManager(self.orcid_path),
                                      doi=CSVManager(self.doi_path), use_api_service=False)
        self.assertIn("0000-0003-0530-4305", cf_2.get_orcid("10.1007/s11192-018-2988-z"))
        self.assertNotIn("0000-0001-5506-523X", cf_2.get_orcid("10.1007/s11192-018-2988-z"))

        # Do not use support files neither APIs
        cf_3 = CrossrefResourceFinder(use_api_service=False)
        self.assertIsNone(cf_3.get_orcid("10.1007/s11192-018-2988-z"))

    def test_crossref_get_issn(self):
        # Do not use support files, only APIs
        cf_1 = CrossrefResourceFinder()
        self.assertIn("0138-9130", cf_1.get_container_issn("10.1007/s11192-018-2988-z"))
        self.assertNotIn("0138-9000", cf_1.get_container_issn("10.1007/s11192-018-2988-z"))

        # Do use support files, but avoid using APIs
        cf_2 = CrossrefResourceFinder(issn=CSVManager(self.issn_path),
                                      doi=CSVManager(self.doi_path), use_api_service=False)
        self.assertIn("1588-2861", cf_2.get_container_issn("10.1007/s11192-018-2988-z"))
        self.assertNotIn("0138-9000", cf_2.get_container_issn("10.1007/s11192-018-2988-z"))

        # Do not use support files neither APIs
        cf_3 = CrossrefResourceFinder(use_api_service=False)
        self.assertIsNone(cf_3.get_container_issn("10.1007/s11192-018-2988-z"))

    def test_crossref_get_pub_date(self):
        # Do not use support files, only APIs
        cf_1 = CrossrefResourceFinder()
        self.assertIn("2019-01-02", cf_1.get_pub_date("10.1007/s11192-018-2988-z"))
        self.assertNotEqual("2019", cf_1.get_pub_date("10.1007/s11192-018-2988-z"))

        # Do use support files, but avoid using APIs
        cf_2 = CrossrefResourceFinder(date=CSVManager(self.date_path),
                                      doi=CSVManager(self.doi_path), use_api_service=False)
        self.assertIn("2019-01-02", cf_2.get_pub_date("10.1007/s11192-018-2988-z"))
        self.assertNotEqual("2018-01-02", cf_2.get_pub_date("10.1007/s11192-018-2988-z"))

        # Do not use support files neither APIs
        cf_3 = CrossrefResourceFinder(use_api_service=False)
        self.assertIsNone(cf_3.get_pub_date("10.1007/s11192-018-2988-z"))

    def test_nationalinstititeofhealth_get_orcid(self):
        #Do not use support files, only APIs
        nf_1 = NIHResourceFinder()
        self.assertIsNone(nf_1.get_orcid("7189714"))
        self.assertIsNone(nf_1.get_orcid("1509982"))

        # Do use support files, but avoid using APIs
        nf_2 = NIHResourceFinder(orcid=CSVManager(self.orcid_path_pmid),
                                      pmid=CSVManager(self.pmid_path), use_api_service=False)
        self.assertIn("0000-0002-1825-0097", nf_2.get_orcid("7189714"))
        self.assertNotIn("0000-0002-1825-0098", nf_2.get_orcid("1509982"))

        # Do not use support files neither APIs
        nf_3 = NIHResourceFinder(use_api_service=False)
        self.assertIsNone(nf_3.get_orcid("7189714"))

    def test_nationalinstititeofhealth_get_issn(self):
        # Do not use support files, only APIs
        nf_1 = NIHResourceFinder()
        self.assertIn("0003-4819", nf_1.get_container_issn("2942070"))
        self.assertNotIn("0003-0000", nf_1.get_container_issn("2942070"))

        # # Do use support files, but avoid using APIs
        nf_2 = NIHResourceFinder(issn=CSVManager(self.issn_path_pmid),
                                      pmid=CSVManager(self.pmid_path), use_api_service=False)
        print("self.issn_path_pmid", self.issn_path_pmid)
        print("self.pmid_path", self.pmid_path)
        container = nf_2.get_container_issn("1509982")
        print("container", container)
        self.assertIn("0065-4299", container)
        self.assertNotIn("0065-4444", nf_2.get_container_issn("1509982"))

        # Do not use support files neither APIs
        nf_3 = NIHResourceFinder(use_api_service=False)
        self.assertIsNone(nf_3.get_container_issn("7189714"))

    def test_nationalinstititeofhealth_get_pub_date(self):
        # Do not use support files, only APIs
        nf_1 = NIHResourceFinder()
        self.assertIn("1998-05-25", nf_1.get_pub_date("9689714"))
        self.assertNotEqual("1998", nf_1.get_pub_date("9689714"))

        # Do not use support files, only APIs
        nf_2 = NIHResourceFinder(date=CSVManager(self.date_path_pmid),
                                      pmid=CSVManager(self.pmid_path), use_api_service=False)
        self.assertIn("1980-06", nf_2.get_pub_date("7189714"))
        self.assertNotEqual("1980-06-22", nf_2.get_pub_date("7189714"))

        # Do not use support files neither APIs
        nf_3 = NIHResourceFinder(use_api_service=False)
        self.assertIsNone(nf_3.get_pub_date("2942070"))
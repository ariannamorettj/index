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
from index.coci.glob import process
from os import sep, makedirs
from os.path import exists
from shutil import rmtree
from index.storer.csvmanager import CSVManager
from index.croci.crowdsourcedcitationsource import CrowdsourcedCitationSource
from csv import DictReader


class CROCITest(unittest.TestCase):

    def setUp(self):
        self.input_file = "index%stest_data%scroci_dump%ssource.csv" % (sep, sep, sep)
        self.citations = "index%stest_data%scroci_dump%scitations.csv" % (sep, sep, sep)

    def test_citation_source(self):
        ccs = CrowdsourcedCitationSource(self.input_file)
        new = []
        cit = ccs.get_next_citation_data()
        while cit is not None:
            citing, cited, creation, timespan, journal_sc, author_sc = cit
            new.append({
                "citing": citing,
                "cited": cited,
                "creation": "" if creation is None else creation,
                "timespan": "" if timespan is None else timespan,
                "journal_sc": "no" if journal_sc is None else journal_sc,
                "author_sc": "no" if author_sc is None else author_sc
            })
            cit = ccs.get_next_citation_data()

        with open(self.citations, encoding="utf8") as f:
            old = list(DictReader(f))

        self.assertEqual(new, old)

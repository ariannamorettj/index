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

from index.finder.resourcefinder import ApiIDResourceFinder
from index.citation.oci import OCIManager
from requests import get
from urllib.parse import quote
from datetime import datetime
from bs4 import BeautifulSoup
import re

class NIHResourceFinder(ApiIDResourceFinder):
    def __init__(self, date=None, orcid=None, issn=None, pmid=None, use_api_service=True):
        self.use_api_service = use_api_service
        self.api = "https://pubmed.ncbi.nlm.nih.gov/"
        self.baseurl = "https://pubmed.ncbi.nlm.nih.gov/"
        super(NIHResourceFinder, self).__init__(date=date, orcid=orcid, issn=issn, id=pmid, id_type=OCIManager.pmid_type,
                                                     use_api_service=use_api_service)

    def _get_issn(self, txt_obj):
        result = set()
        issns = re.findall("IS\s+-\s+\d{4}-\d{4}", txt_obj)
        for i in issns:
            issn = re.search("\d{4}-\d{4}", i).group(0)
            result.add(issn)
        return result

    def _get_date(self, txt_obj):
        date = re.search("DP\s+-\s+(\d{4}(\s?(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec))?(\s?((3[0-1])|([1-2][0-9])|([0]?[1-9])))?)", txt_obj).group(1)
        re_search = re.search("(\d{4})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+((3[0-1])|([1-2][0-9])|([0]?[1-9]))", date)
        if re_search is not None:
            result = re_search.group(0)
            datetime_object = datetime.strptime(result, '%Y %b %d')
            return datetime.strftime(datetime_object, '%Y-%m-%d')
        else:
            re_search = re.search("(\d{4})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)", date)
            if re_search is not None:
                result = re_search.group(0)
                datetime_object = datetime.strptime(result, '%Y %b')
                return datetime.strftime(datetime_object, '%Y-%m')
            else:
                re_search = re.search("(\d{4})", date)
                if re_search is not None:
                    result = re.search("(\d{4})", date).group(0)
                    datetime_object = datetime.strptime(result, '%Y')
                    return datetime.strftime(datetime_object, '%Y')
                else:
                    return None


    def _call_api(self, pmid_full):
        if self.use_api_service:
            pmid = self.pm.normalise(pmid_full)
            r = get(self.api + quote(pmid) + "/?format=pubmed", headers=self.headers, timeout=30)
            if r.status_code == 200:
                r.encoding = "utf-8"
                soup = BeautifulSoup(r.text, features="lxml")
                mdata = str(soup.find(id="article-details"))
                return mdata
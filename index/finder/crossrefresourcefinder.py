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
from json import loads
import index.support.dictionary as sd
from datetime import datetime


class CrossrefResourceFinder(ApiIDResourceFinder):
    def __init__(self, date=None, orcid=None, issn=None, doi=None, use_api_service=True):
        self.use_api_service = use_api_service
        self.api = "https://api.crossref.org/works/"
        super(CrossrefResourceFinder, self).__init__(date=date, orcid=orcid, issn=issn, id=doi, id_type=OCIManager.doi_type,
                                                     use_api_service=use_api_service)

    def _get_orcid(self, json_obj):
        result = set()

        if json_obj is None:
            return result
        for author in json_obj.get("author", []):
            orcid = self.om.normalise(author.get("ORCID"))
            if orcid is not None:
                result.add(orcid)

        return result

    def _get_issn(self, json_obj):
        result = set()

        if json_obj is None:
            return result
        if not sd.contains(json_obj, "type", "journal"):
            return result
        for issn in json_obj.get("ISSN", []):
            norm_issn = self.im.normalise(issn)
            if norm_issn is not None:
                result.add(norm_issn)

        return result

    def _get_date(self, json_obj):
        if json_obj is None:
            return
        date = json_obj.get("issued")
        if not date:
            return
        date_list = date["date-parts"][0]
        if date_list is None:
            return
        l_date_list = len(date_list)
        if l_date_list != 0 and date_list[0] is not None:
            if l_date_list == 3 and \
                    ((date_list[1] is not None and date_list[1] != 1) or
                     (date_list[2] is not None and date_list[2] != 1)):
                result = datetime(date_list[0], date_list[1], date_list[2], 0, 0).strftime('%Y-%m-%d')
            elif l_date_list == 2 and date_list[1] is not None:
                result = datetime(date_list[0], date_list[1], 1, 0, 0).strftime('%Y-%m')
            else:
                result = datetime(date_list[0], 1, 1, 0, 0).strftime('%Y')

            return result

    def _call_api(self, doi_full): #uniformato alla versione corrente di oc
        if self.use_api_service:
            doi = self.dm.normalise(doi_full)
            r = get(self.api + quote(doi), headers=self.headers, timeout=30)
            if r.status_code == 200:
                r.encoding = "utf-8"
                return r.json().get("message")

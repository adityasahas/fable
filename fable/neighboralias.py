"""
Prepare sheet for study infer back.
Given the original broken URL
1). Looking for other broken URLs in the same format
2). Pick broken and close ones (with hist redir)
3). Format into a csv for testing
"""
import os
from re import L
from urllib.parse import urlsplit, parse_qs
import random
from dateutil import parser as dparser
import threading
from statistics import median

from . import config, tools, searcher, histredirector, inferer, tracer
from fable.utils import url_utils, crawl, sic_transit

he = url_utils.HostExtractor()

class NeighborAlias:
    def __init__(self):
        self.memo = tools.Memoizer()
        
    def __detect_str_alnum(self, string):
        """Detect whether string has alpha and/or numeric char"""
        typee = ''
        alpha_char = [c for c in string if c.isalpha()]
        num_char = [c for c in string if c.isdigit()]
        if len(alpha_char) > 0:
            typee += 'A'
        if len(num_char) > 0:
            typee += 'N'
        return typee

    def _get_filename_alnum(self, url):
        path = urlsplit(url).path
        filename = list(filter(lambda x: x!='', path.split('/')))
        if len(filename) == 0:
            filename = ''
        else:
            filename = filename[-1]
        filename, _ = os.path.splitext(filename)
        return self.__detect_str_alnum(filename)

    def _length(self, url):
        path = urlsplit(url).path
        path = list(filter(lambda x: x!='', path.split('/')))
        return len(path)

    def _same_pattern(self, url1, url2):
        # * Filter out same urls
        if url_utils.url_match(url1, url2):
            return False
        # return url_utils.netloc_dir(url1)==url_utils.netloc_dir(url2) \
        #         and _length(url1)==_length(url2)\
        #         and _get_filename_alnum(url1)==_get_filename_alnum(url2)
        return self._length(url1)== self._length(url2)
                # and self._get_filename_alnum(url1)== self._get_filename_alnum(url2)

    def _order_neighbors(self, target_urls, neighbors, ts):
        """Order the neighbors so that most similar neighbor (in location/format and in time) can be tested first"""
        all_neighbors = []
        target_urls = random.sample(target_urls, min(5, len(target_urls)))
        print("Sampled target urls:", target_urls)
        for target_url in target_urls:
            all_neighbors += url_utils.order_neighbors(target_url, neighbors, urlgetter=lambda x: x[1], ts=ts)
        all_neighbors.sort(key=lambda x: x[2][0], reverse=True)
        # * dedup
        uniq_neighbor_score, seen = [], set()
        for neighbor in all_neighbors:
            keyneighbor = url_utils.url_norm(neighbor[1], wayback=True, case=True, trim_www=True,\
                trim_slash=True, ignore_scheme=True)
            if keyneighbor in seen:
                continue
            seen.add(keyneighbor)
            uniq_neighbor_score.append(neighbor)
        return uniq_neighbor_score

    def _non_broken_alias(self, url):
        """Assume the url is not broken"""
        html, final_url = self.memo.crawl(url, final_url=True)
        if final_url and not url_utils.url_match(url, final_url):
            return crawl.get_canonical(final_url, html)
        return

    def get_neighbors(self, urls, tss=[], status_filter='23'):
        """Get neighbors (in order)"""
        url = urls[0]
        netdir = url_utils.netloc_dir(url, exclude_index=True)
        url_dir = netdir[1]
        count = 0
        neighbors = []
        seen_neighbors = set()
        while count < 3 and url_dir != "/" and len(seen_neighbors) < 10:
            q = netdir[0] + url_dir + '/*'
            param_dict = {
                'url': q,
                'filter': ['mimetype:text/html', f'statuscode:[{status_filter}][0-9]*'],
                # 'collapse': ['urlkey'],
                'output': 'json',
            }
            w, _ = crawl.wayback_index(q, param_dict=param_dict)
            print(f"First query {q}: {len(w)}")
            same_w = [ww for ww in w if self._same_pattern(url, ww[1])]
            print(f"Second pattern: {len(same_w)}")
            neighbors += same_w
            seen_neighbors = set([u[1] for u in neighbors])
            count += 1
            url_dir = url_utils.nondigit_dirname(url_dir)
        
        tss = [url_utils._safe_dparse(ts) for ts in tss if ts and isinstance(ts, str)]
        ts = median(tss) if len(tss) > 0 else None
        ordered_w = self._order_neighbors(urls, neighbors, ts)
        print('length ordered_w', len(ordered_w))
        return ordered_w
"""
Check for wayback alias
"""
import os
from urllib.parse import urlsplit, urlparse, parse_qsl, parse_qs, urlunsplit
from bs4 import BeautifulSoup
from queue import Queue
from collections import defaultdict
import re, json
import requests
from dateutil import parser as dparser
import datetime

from . import config, tools, tracer
from .utils import crawl, url_utils, sic_transit, text_utils

import logging
logging.setLoggerClass(tracer.tracer)
tracer = logging.getLogger('logger')
logging.setLoggerClass(logging.Logger)

he = url_utils.HostExtractor()

def _safe_dparse(ts):
    try:
        return dparser.parse(ts)
    except:
        return datetime.datetime.now()

class HistRedirector:
    def __init__(self, corpus=[], proxies={}, memo=None):
        self.corpus = corpus
        self.PS = crawl.ProxySelector(proxies)
        self.memo = memo if memo is not None else tools.Memoizer()
        self.prefix_wayback_300s = {} # * Cache prefix searched 300 archives
        self.crawl_cache = {} # * Cache crawled response
        self.wayback_index_cache = defaultdict(list) # * Cache wayback indexed results

    def _requests_crawl(self, url):
        if url in self.crawl_cache:
            return self.crawl_cache[url]
        else:
            resp = crawl.requests_crawl(url, raw=True)
            self.crawl_cache[url] = resp
            return resp
    
    def _wayback_index(self, url, non_400=True):
        url = url_utils.url_norm(url)
        if len(self.wayback_index_cache):
            wayback_cache = self.wayback_index_cache.get(url, [])
            non_400_dict = {True: '23', False: '4'}
            wayback_cache = [wc for wc in wayback_cache if wc[2][0] in non_400_dict[non_400]]
            return wayback_cache
        else:
            if non_400: # * Case for [23][00]
                waybacks = self.memo.wayback_index(url, policy='all', all_none_400=True)
                return waybacks
            else: # * Live crawl
                param_dict = {
                    'filter': ['mimetype:text/html', 'statuscode:[4][0-9]*']
                }
                waybacks, _ = crawl.wayback_index(url, param_dict=param_dict)
                return waybacks

    def _order_neighbors(self, target_url, neighbors, ts):
        """Order the neighbors so that most similar neighbor (in location/format and in time) can be tested first"""
        lambdas = []
        def directory_close(target_url, url):
            path = urlsplit(url).path
            directory = url_utils.nondigit_dirname(path)
            directory = directory.split('/')[1:]
            target_path = urlsplit(target_url).path
            target_directory = url_utils.nondigit_dirname(target_path)
            target_directory = target_directory.split('/')[1:]
            return len(set(target_directory).intersection(directory)) /  len(set(target_directory).union(directory))           

        # * Directory closeness
        lambdas.append(lambda x: -directory_close(target_url, x))
        neighbor_score = url_utils.order_neighbors(target_url, neighbors, 
                            urlgetter=lambda x: x[1], ts=ts, prefix_funcs=[])
        # * dedup
        uniq_neighbor_score, seen = [], set()
        for i, neighbor in enumerate(neighbor_score):
            if url_utils.url_norm(neighbor[1], wayback=True, trim_slash=True, trim_www=True) in seen:
                continue
            seen.add(url_utils.url_norm(neighbor[1], wayback=True, trim_slash=True, trim_www=True))
            uniq_neighbor_score.append(neighbor)
        # tracer.debug(uniq_neighbor_score[:10])
        return uniq_neighbor_score

    def _verify_alias(self, url, new_urls, ts, homepage_redir, strict_filter, require_neighbor, live_working, seen_redir_url):
        """
        Verify whether new_url is valid alias by checking:
            1. new_urls is in the same site
            2. new_urls is working 
            3. whether there is no other url in the same form redirected to this url
        """
        global tracer
        new_url = new_urls[-1]

        # * If new url is in the same site
        orig_host = he.extract(url)
        host_url = f'http://{orig_host}'
        new_host = he.extract(new_url)
        new_host_url = f'http://{new_host}'
        _, orig_host = self.memo.crawl(host_url, final_url=True)
        _, new_host = self.memo.crawl(new_host_url, final_url=True)
        if orig_host is None or new_host is None or he.extract(new_host) != he.extract(orig_host):
            tracer.debug('verify_alias: redirected URL not in the same site')
            return False

        # *If homepage to homepage redir, no soft-404 will be checked
        # ? If live_working == False, no need to check for breakeage
        if live_working:
            broken, _ = sic_transit.broken(new_url, html=True, ignore_soft_404_content=homepage_redir)
            if broken: return False
            if homepage_redir: return True
        # ? End of live_working
        
        if isinstance(ts, str): ts = dparser.parse(ts)
        ts_year = ts.year

        # * Perform strict filter if set to true
        if strict_filter:
            new_us = urlsplit(new_url)
            us = urlsplit(url)
            if not new_us.query and not us.query and new_us.path in us.path:
                return False
        
        # *If url ended with / (say /dir/), consider both /* and /dir/*
        url_prefix = urlsplit(url)
        url_dir = url_utils.nondigit_dirname(url_prefix.path)
        count = 0
        not_match = lambda u: not url_utils.url_match(url, url_utils.filter_wayback(u) ) # and not url_utils.filter_wayback(u)[-1] == '/'
        _path_length = lambda url: len(list(filter(lambda x: x != '', urlsplit(url).path.split('/'))))
        same_length = lambda u: _path_length(url) == _path_length(url_utils.filter_wayback(u))
        neighbors, neighbor_set = [], set()
        while count < 3 and url_dir != "/" and len(neighbor_set) < 5:
        # // if url_prefix.path[-1] == '/': url_dir.append(os.path.dirname(url_dir[0]))
            url_prefix = url_prefix._replace(path=os.path.join(url_dir, '*'), query='', fragment='')
            url_prefix_str = urlunsplit(url_prefix)
            if url_prefix_str not in self.prefix_wayback_300s:
                param_dict = {
                    # 'from': str(ts_year) + '0101',
                    # 'to': str(ts_year) + '1231',
                    "filter": ['statuscode:3[0-9]*', 'mimetype:text/html'],
                    # 'limit': 1000
                }
                # TODO: Make this supported by cache_index
                neighbor, _ = crawl.wayback_index(url_prefix_str, param_dict=param_dict, total_link=True)
                tracer.debug(f'Search for neighbors with query & year: {url_prefix_str} {ts_year}. Count: {len(neighbor)}')
                self.prefix_wayback_300s[url_prefix_str] = neighbor
            else:
                neighbor = self.prefix_wayback_300s[url_prefix_str]
                tracer.debug(f'Cached neighbors with query & year: {url_prefix_str} {ts_year}. Count: {len(neighbor)}')

            # *Get closest crawled urls in the same dir, which is not target itself  
            same_netdir = lambda u: url_dir in url_utils.nondigit_dirname(urlsplit(url_utils.filter_wayback(u)).path[:-1])
            neighbor = [n for n in neighbor if not_match(n[1]) \
                                                    and same_netdir(n[1]) \
                                                    and same_length(n[1]) ]
            neighbor_set.update([url_utils.filter_wayback(n[1]) for n in neighbor])

            neighbors += neighbor
            count += 1
            url_dir = url_utils.nondigit_dirname(url_dir)

        neighbors = self._order_neighbors(url, neighbors, ts)
        # tracer.debug(f'neightbor: {len(neighbor)}')
        tracer.debug(neighbors[:10])
        matches = []
        for i in range(min(5, len(neighbors))):
            try:
                tracer.debug(f'Choose closest neighbor: {neighbors[i][1]}')
                response = self._requests_crawl(neighbors[i][1])
                neighbor_urls = [r.url for r in response.history[1:]] + [response.url]
                if (url_utils.url_match(neighbors[i][1], response.url, wayback=True)):
                    tracer.debug(f'No actual redirection')
                    continue
                match = False

                # ? If live_working == False, no need to check for liveweb neighbor
                if live_working:
                    live_neighor_response = self._requests_crawl(url_utils.filter_wayback(response.url))
                    live_neighor_url, html = live_neighor_response.url, live_neighor_response.text
                    live_neighor_url = crawl.get_canonical(live_neighor_url, html)
                    neighbor_urls.append(live_neighor_url)
                # ? End of live_working

                for neighbor_url in neighbor_urls:
                    for new_url in new_urls:    
                        thismatch = url_utils.url_match(new_url, neighbor_url)
                        if thismatch: 
                            match = True
                            seen_redir_url.add(new_url)
                matches.append(match)
                if True in matches:
                    tracer.debug(f'url in same dir: {neighbors[i][1]} redirects to the same url')
                    return False
                if len(matches) > 1: # * Chech for two neighbors
                    break
            except Exception as e:
                tracer.debug(f'Cannot check neighbor on wayback_alias: {str(e)}')
                continue
        if require_neighbor and len(matches) == 0:
            tracer.debug(f'require_neighbor is set to True, but there are no neighbors that can be checked')
            return False
        return True

    def wayback_alias_history(self, url, require_neighbor=False, homepage_redir=True, 
                                live_working=True, strict_filter=False):
        """
        Utilize wayback's archived redirections to find the alias/reorg of the page
        Not consider non-homepage to homepage
        If latest redirection is invalid, iterate towards earlier ones (separate by every month)
        require_neighbor: Whether a redirection neighbor is required to do the comparison
        homepage_redir: Whether redirection to homepage (from non-homepage) is considered valid
        live_working: Require the live version of the "alias" to be working. Default set to true
        strict_filter: Not consider case where: redirected URL's path is a substring of the original one

        Returns: List of all redirection history to live version of alias, else None
        """
        tracer.debug('Start wayback_alias')
        us = urlsplit(url)
        is_homepage = us.path in ['/', ''] and not us.query
        try:
            wayback_ts_urls = self._wayback_index(url, non_400=True)
        except: return

        if not wayback_ts_urls or len(wayback_ts_urls) == 0:
            return

        wayback_ts_urls = [(_safe_dparse(c[0]), c[1]) for c in wayback_ts_urls]

        # * Check for 400 snapshots, any redirections after it will not be counted
        broken_archives = self._wayback_index(url, non_400=False)
        if len(broken_archives):
            broken_ts = _safe_dparse(broken_archives[0][0])
            wayback_ts_urls = [w for w in wayback_ts_urls if w[0] < broken_ts]
            if len(wayback_ts_urls) == 0:
                return
            it = len(wayback_ts_urls) - 1
        
        # * Count for unmatched wayback final url, and wayback_alias to same redirected fake alias
        url_match_count, same_redir = 0, 0
        it = len(wayback_ts_urls) - 1
        last_ts = wayback_ts_urls[-1][0] + datetime.timedelta(days=90)
        seen_redir_url = set()
        while url_match_count < 3 and same_redir < 5 and it >= 0:
            ts, wayback_url = wayback_ts_urls[it]
            tracer.debug(f'wayback_alias iteration: ts: {ts} it: {it}')
            it -= 1
            if ts + datetime.timedelta(days=90) > last_ts: # 2 snapshots too close
                continue
            try:
                response = crawl.requests_crawl(wayback_url, raw=True)
                wayback_url = response.url
                # * First match check
                match = url_utils.url_match(url, url_utils.filter_wayback(wayback_url))
                if match:
                    redir_url = text_utils.parse_wayback_redir(response.text)
                    tracer.debug(f"wayback alias: redir URL after parsing HTML: {redir_url}")
                    wayback_url = redir_url if redir_url else wayback_url
                    match =  url_utils.url_match(url, url_utils.filter_wayback(wayback_url))
                    tracer.debug(f"url {url} match with redir {url_utils.filter_wayback(wayback_url)}: {match}")
            except:
                continue

            # *Not match means redirections, the page could have a temporary redirections to the new page
            if match:
                url_match_count += 1
                continue
            last_ts = ts
            new_url = url_utils.filter_wayback(wayback_url)
            inter_urls = [url_utils.filter_wayback(wu.url) for wu in response.history] # Check for multiple redirections
            inter_urls.append(new_url)
            inredir = False
            for inter_url in inter_urls[1:]:
                if inter_url in seen_redir_url:
                    inredir = True
            if inredir:
                same_redir += 1
                continue
            else:
                seen_redir_url.add(new_url)
            inter_uss = [urlsplit(inter_url) for inter_url in inter_urls]
            tracer.info(f'Wayback_alias: {ts}, {inter_urls}')

            # *If non-home URL is redirected to homepage, it should not be a valid redirection
            new_is_homepage = True in [inter_us.path in ['/', ''] and not inter_us.query for inter_us in inter_uss]
            if not homepage_redir and new_is_homepage and (not is_homepage): 
                continue
            
            live_new_url = inter_urls[-1]
            live_new_url = self.na_alias(live_new_url, live_working)
            if live_new_url is None or url_utils.suspicious_alias(url, live_new_url):
                continue
            inter_urls.append(live_new_url)
            # //pass_check, reason = sic_transit.broken(new_url, html=True, ignore_soft_404=is_homepage and new_is_homepage)
            # //ass_check = not pass_check
            if len(inter_urls) > 1:
                inter_urls = inter_urls[1:]
            pass_check = self._verify_alias(url, inter_urls, ts, homepage_redir=is_homepage and new_is_homepage, \
                                            strict_filter=strict_filter, require_neighbor=require_neighbor, \
                                            live_working=live_working, seen_redir_url=seen_redir_url)
            if pass_check:
                # * Select all historical redirected URLs that are still working
                tracer.debug(f'found: {live_new_url}')
                inter_urls = list(dict.fromkeys([url_utils.url_norm(iu, ignore_scheme=True) for iu in inter_urls]))
                working_inter_urls = inter_urls.copy()
                # ? If live_working == False, no need to crawl the liveweb
                if live_working:
                    for iu in inter_urls:
                        r = crawl.requests_crawl(iu, raw=True)
                        if isinstance(r, requests.Response) and \
                            (url_utils.url_match(r.url, inter_urls[-1]) or url_utils.url_match(r.url, live_new_url)):
                            break
                        working_inter_urls.pop(0)
                # ? End of live_working
                return working_inter_urls
        return

    def wayback_alias(self, url, require_neighbor=False, homepage_redir=True, strict_filter=False):
        """
        Wrapper for wayback_alias_history
        
        Return: If found an alias, only return the live web version of the alias, else None
        """
        alias = self.wayback_alias_history(url, require_neighbor=require_neighbor, \
                        homepage_redir=homepage_redir, strict_filter=strict_filter)
        if isinstance(alias, list):
            alias = alias[-1]
        return alias
    
    def wayback_alias_batch_history(self, urls, require_neighbor=False, homepage_redir=True, strict_filter=False):
        """
        Run wayback_alias on the list of URL. (URLs need to be under the same directory)
        Save every individual effort on first wayback indexing + crawling responses + following query wayback

        Return: {url: results as wayback_alias}
        """
        self.prefix_wayback_300s = {}
        self.crawl_cache = {}

        # * Query all 300 archives once
        if len(self.wayback_index_cache) == 0:
            cur_prefix = max(urls, key=lambda x: len(x))
            param_dict = {
                'filter': ['mimetype:text/html'],
                'output': 'json'
            }
            for url in urls:
                url_prefix = url_utils.netloc_dir(url, exclude_index=True)
                if len(url_prefix) < len(cur_prefix):
                    cur_prefix = url_prefix
            cur_prefix = cur_prefix[0] + cur_prefix[1]
            tracer.debug(f"batch_history: wayback index with prefix: {cur_prefix + '/*'}")
            waybacks, _ = crawl.wayback_index(cur_prefix + '/*', param_dict=param_dict, total_link=True)
            for wayback in waybacks:
                target_url = url_utils.filter_wayback(wayback[1])
                self.wayback_index_cache[url_utils.url_norm(target_url)].append(wayback)

        url_history = {}
        for url in urls:
            alias = self.wayback_alias_history(url, require_neighbor=require_neighbor, \
                        homepage_redir=homepage_redir, strict_filter=strict_filter)
            url_history[url] = alias
        return url_history

    def wayback_alias_batch(self, urls, require_neighbor=False, homepage_redir=True, strict_filter=False):
        """Wrapper for wayback_alias_batch_history"""
        results = {}
        url_hist = self.wayback_alias_batch_history(urls, require_neighbor=require_neighbor, \
                        homepage_redir=homepage_redir, strict_filter=strict_filter)
        for url, alias in url_hist.items():
            if isinstance(alias, list):
                alias = alias[-1]
            results[url] = alias
        return results
    
    def wayback_alias_any_history(self, url, require_neighbor=False, homepage_redir=True, strict_filter=False):
        """Find for historical redirection with no requirement for live web working"""
        alias = self.wayback_alias_history(url, require_neighbor=require_neighbor, \
                        homepage_redir=homepage_redir, strict_filter=strict_filter, live_working=False)
        return alias
    
    def wayback_alias_batch_any_history(self, urls, require_neighbor=False, homepage_redir=True, strict_filter=False):
        """
        Run wayback_alias on the list of URL. (URLs need to be under the same directory)
        Save every individual effort on first wayback indexing + crawling responses + following query wayback

        Return: {url: results as wayback_alias}
        """
        self.prefix_wayback_300s = {}
        self.crawl_cache = {}

        # * Query all 300 archives once
        if len(self.wayback_index_cache) == 0:
            cur_prefix = max(urls, key=lambda x: len(x))
            param_dict = {
                'filter': ['mimetype:text/html'],
                'output': 'json'
            }
            for url in urls:
                url_prefix = url_utils.netloc_dir(url, exclude_index=True)
                if len(url_prefix) < len(cur_prefix):
                    cur_prefix = url_prefix
            cur_prefix = cur_prefix[0] + cur_prefix[1]
            tracer.debug(f"batch_history: wayback index with prefix: {cur_prefix + '/*'}")
            waybacks, _ = crawl.wayback_index(cur_prefix + '/*', param_dict=param_dict, total_link=True)
            for wayback in waybacks:
                target_url = url_utils.filter_wayback(wayback[1])
                self.wayback_index_cache[url_utils.url_norm(target_url)].append(wayback)

        url_any_history = {}
        for url in urls:
            alias = self.wayback_alias_any_history(url, require_neighbor=require_neighbor, \
                        homepage_redir=homepage_redir, strict_filter=strict_filter)
            url_any_history[url] = alias
        return url_any_history

    def na_alias(self, alias, live_working):
        """Check whether found alias are N/A"""
        # * If today's url is not in the same site, not a valid redirection
        new_host = he.extract(alias)
        new_host_url = f'http://{new_host}'
        _, new_host_url = self.memo.crawl(new_host_url, final_url=True)
        if new_host_url is None:
            new_host_url = f'http://{new_host}'
        # ? If live_working == False, no need to crawl liveweb
        if live_working:
            html, alias = self.memo.crawl(alias, final_url=True)
            tracer.debug(f"Alias, new_host_url {alias} {new_host_url}")
            alias = crawl.get_canonical(alias, html)
            if not alias or he.extract(new_host_url) != he.extract(alias):
                tracer.debug(f"no alias: {alias} not in the same site as the original site {new_host}")
                return
        # ? End of live_working
        
        # * Check if alias is a login page
        if url_utils.na_url(alias):
            tracer.debug(f"no_alias: filename includes unwanted keyword")
            return
        return alias
    
    def get_title_atitle(self, urls):
        """
        For each input URL, get (title, title after historical redirection if available)
        """
        islist = isinstance(urls, list)
        url_title = defaultdict(lambda: [None,None])
        if not islist: urls = [urls]
        for url in urls:
            wayback_url = self.memo.wayback_index(url)
            if wayback_url: 
                wayback_html = self.memo.crawl(wayback_url)
                title = self.memo.extract_title(wayback_html)
                url_title[url][0] = title
        url_any_alias = self.wayback_alias_batch_any_history(urls)
        for url, any_alias in url_any_alias.items():
            if any_alias:
                wayback_aalias = self.memo.wayback_index(any_alias)
                wayback_ahtml = self.memo.crawl(wayback_aalias)
                atitle = self.memo.extract_title(wayback_ahtml)
                url_title[url][1] = atitle
        return url_title
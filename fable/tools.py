"""
Functions for determines whether two pages are similar/same
Methodologies: Content Match / Parital Match
"""
import pymongo
from pymongo import MongoClient
import brotli
import re, os
import regex
import time
from collections import defaultdict
import random
import brotli
from dateutil import parser as dparser
import datetime
from urllib.parse import urlsplit, urlunsplit, parse_qsl
import bisect
from bs4 import BeautifulSoup

from . import config, tracer
from .utils import text_utils, crawl, url_utils, search
from .utils.url_utils import url_norm
from .utils.sic_transit import text_norm

import logging
logging.setLoggerClass(tracer.tracer)
tracer = logging.getLogger('logger')
logging.setLoggerClass(logging.Logger)

DEFAULT_CACHE = 3600*24*30
LEAST_SITE_URLS = 20 # Least # of urls a site must try to crawl to enable title comparison
COMMON_TITLE_SIZE = 5 # Common prefix/suffix extraction's sample number of title

VERTICAL_BAR_SET = '\u007C\u00A6\u2016\uFF5C\u2225\u01C0\u01C1\u2223\u2502\u0964\u0965'
OTHER_DELIMITER_SET = '::'

he = url_utils.HostExtractor()

def update_sites(collection):
    global he
    no_sites = list(collection.find({'site': {'$exists': False}}))
    for no_site in no_sites:
        site = he.extract(no_site['url'], wayback='web.archive.org' in no_site['url'])
        try:
            collection.update_one({'_id': no_site['_id']}, {'$set': {'site': site}})
        except: pass


def title_common(titles):
    """Extract common parts of titles. Returns: set of common token"""
    if len(titles) == 0:
        return []
    common = set(re.split('_| \| |\|| - |-', titles[0]))
    for t in titles[1:]:
        common = common.intersection(re.split('_| \| |\|| - |-', t))
    return common


def different_page(url, meta, content, crawls, wayback=False):
    """
    Return pages with differnt content in crawls, that has closest title to it
    meta: metadata to identify index, title if not wayback, ts otherwise
    wayback: Whether to consider ts
    """
    if not wayback:
        crawl_meta = [c['title'] for c in crawls]
    else:
        crawl_meta = [c['ts'] for c in crawls]
    left_idx = bisect.bisect(crawl_meta, meta)
    if left_idx >= len(crawls): left_idx -= 1
    right_idx = left_idx + 1
    left, right = left_idx >= 0, right_idx < len(crawls)
    max_content_simi = 0
    while left or right:
        if left:
            crawl_left = crawls[left_idx]
            # TODO: Content comparison slows the process a lot
            content_simi = text_utils.k_shingling(content, crawl_left.get('content', ''))
            if not url_utils.url_match(url, crawl_left['url']) \
              and (content_simi < 0.9 or max_content_simi >= 0.9): # * If three pages all have similar content, something wrong
                return crawl_left
            max_content_simi = max(max_content_simi, content_simi)
            left_idx -= 1
            left = left_idx >= 0
        if right:
            crawl_right = crawls[right_idx]
            content_simi = text_utils.k_shingling(content, crawl_right.get('content', ''))
            if not url_utils.url_match(url, crawl_right['url']) \
              and (content_simi < 0.9 or max_content_simi >= 0.9): # * If three pages all have similar content, something wrong
                return crawl_right
            max_content_simi = max(max_content_simi, content_simi)
            right_idx += 1
            right = right_idx < len(crawls)
    

def title_prepare(crawls, wayback=False):
    """
    Prepapre required data structures for unique_title
    crawls: URLs' crawls with title, HTML, content (if applicable)
    wayback: whether the common prefix/suffix extraction is for wayback urls. If set to False, mean liveweb pages.
    
    Returns: site_meta
        
    """
    netloc_dir = defaultdict(list)
    memo = Memoizer()
    for ut in crawls:
        if 'content' not in ut:
            try:
                html = brotli.decompress(ut['html']).decode()
                ut['content'] = memo.extract_content(html, version='boilerpipe', handle_exception=False)
            except: pass
        if wayback:
            ut.update({
                'url': url_utils.filter_wayback(ut['url']),
                'ts': url_utils.get_ts(ut['url'])
            })
        nd = url_utils.netloc_dir(ut['url'])
        netloc_dir[nd].append(ut)
    url_meta = [[k, v] for k, v in netloc_dir.items()]
    url_meta.sort(key=lambda x: x[0])
    # * Sort the crawls in the same netloc_dir by title, so that same title are put together
    for i in range(len(url_meta)):
        if not wayback:
            url_meta[i][1] = sorted(url_meta[i][1], key=lambda x: x['title'])
        else:
            url_meta[i][1] = sorted(url_meta[i][1], key=lambda x: int(x['ts']))
    return url_meta


def token_intersect(title1_token, title2_token):
    """Intersection on two titles' token. Return intersections if available"""
    len1, len2 = len(title1_token), len(title2_token)
    len_m = len1 * len2
    if len_m > 1 and len_m in [len1, len2]: # * One don't have separator, the other have
        return set()
    elif len_m > 1: # *Both with separator
        return set(title1_token).intersection(title2_token)
    else: # *Both without separator
        return set()
        return set(title1_token[0].split()).intersection(title2_token[0].split())


def unique_title(url, title, content, site_url_meta, wayback=False, return_common_part=False):
    """
    Eliminate common suffix/prefix of certain site for liveweb
    url: full url (if wayback, url including web.archive.org)
    site_url_meta: [[netloc_dir, [crawls]]] sorted in netloc_dir
    
    Returns: prefix/suffix filtered title, prefix/suffix if return_common_part is True
    """
    title_tokens = regex.split(f'_| [{VERTICAL_BAR_SET}] |[{VERTICAL_BAR_SET}]| \p{{Pd}} |\p{{Pd}}| (?:{OTHER_DELIMITER_SET}) |(?:{OTHER_DELIMITER_SET})', title)
    title_tokens = [t.strip() for t in title_tokens]
    if wayback:
        url, ts = url_utils.filter_wayback(url), url_utils.get_ts(url)
    nd = url_utils.netloc_dir(url)
    close_idx = bisect.bisect_left(site_url_meta, [nd, []])
    if close_idx == len(site_url_meta):
        close_idx -= 1 # * In case close_idx is the out of bound
    upidx, downidx = close_idx, close_idx + 1
    diffs = set() # *{common_prefix_diff has seen, when -3 to 3 all seen, quit}
    itsts = set()
    striphost = lambda nd: nd[0].split(':')[0]

    # * Find first candidate which is:
    # *   1. not url itself
    # *   2. has same hostname (www excluded)
    upgoing = not (upidx < 0 \
                or striphost(site_url_meta[upidx][0]) != nd[0])
    downgoing = not (downidx > len(site_url_meta) - 1 \
                  or striphost(site_url_meta[downidx][0]) != nd[0])
    

    meta = ts if wayback else title
    while (upgoing or downgoing) and len(diffs) < 7:
        tocheck = []
        if upgoing:
            upcrawl = different_page(url, meta, content, site_url_meta[upidx][1], wayback=wayback)
            if upcrawl:
                upurl, uptitle = upcrawl['url'], upcrawl['title']
                upurl = url_utils.url_norm(upurl)
                uptd = url_utils.common_prefix_diff(url, upurl)
                if abs(uptd) <= 3 and not url_utils.url_match(upurl, url):
                    tocheck.append((uptd, upurl, uptitle))
        if downgoing:
            downcrawl = different_page(url, meta, content, site_url_meta[downidx][1], wayback=wayback)
            if downcrawl:
                downurl, downtitle = downcrawl['url'], downcrawl['title']
                downurl = url_utils.url_norm(downurl)
                downtd = url_utils.common_prefix_diff(url, downurl)
                if abs(downtd) <= 3 and not url_utils.url_match(downurl, url):
                    tocheck.append(((downtd, downurl, downtitle)))

        if len(tocheck) > 1: # *Put small in front
            tocheck[0], tocheck[1] = min(tocheck, key=lambda x:x[0]), max(tocheck, key=lambda x:x[0])
        for td, cand_url, cand_title in tocheck:
            diffs.add(td)
            cand_title_tokens = regex.split(f'_| [{VERTICAL_BAR_SET}] |[{VERTICAL_BAR_SET}]| \p{{Pd}} |\p{{Pd}}| (?:{OTHER_DELIMITER_SET}) |(?:{OTHER_DELIMITER_SET})', cand_title)
            cand_title_tokens = [t.strip() for t in cand_title_tokens]
            itsts = token_intersect(title_tokens, cand_title_tokens)
            if len(itsts) > 0:
                break
        if len(itsts) > 0:
            break

        upidx -= 1
        downidx += 1
        upgoing = upgoing and not (upidx < 0 \
                        or striphost(site_url_meta[upidx][0]) != nd[0])
        downgoing = downgoing and not (downidx > len(site_url_meta) - 1 \
                        or striphost(site_url_meta[downidx][0]) != nd[0])
 
    utitle = ''
    if len(itsts) <= 0:
        utitle = title
    elif len(title_tokens) > 1:
        utitle = ' '.join([tt for tt in title_tokens if tt not in itsts])
    else:
        utitle = ' '.join([tt for tt in title_tokens[0].split() if tt not in itsts])
    utitle = utitle.strip()
    tracer.debug(f'unique_title: {url} --> "{utitle}"')
    return utitle


def norm_path(url):
    us = urlsplit(url)
    if not us.query:
        return us.path
    else:
        return f"{us.path}?{us.query}"


def date_parse(date):
    """Wrapper around dparser.parse to handle exceptions"""
    trim = [len(date), 8, 4]
    for t in trim:
        try:
            d = dparser.parse(date[:t])
            return d
        except:
            pass
    return datetime.datetime.now()

class Memoizer:
    """
    Class for reducing crawl and wayback indexing
    """
    def __init__(self, use_db=True, db=None, proxies={}):
        """
        # TODO: Implement non-db version. (In mem version)
        """
        self.use_db = use_db
        if use_db:
            self.db = config.new_db() if not db else db
        self.PS = crawl.ProxySelector(proxies)
    
    def crawl(self, url, final_url=False, max_retry=0, **kwargs):
        """
        final_url: Whether also return final redirected URLS
        max_retry: Number of max retry times
        TODO: non-db version
        """
        is_wayback = 'web.archive.org/web' in url
        if not final_url:
            html = self.db.crawl.find_one({'_id': url})
        else:
            html = self.db.crawl.find_one({'_id': url, 'final_url': {"$exists": True}})
        if html and (html['ttl'] > time.time() or is_wayback):
            tracer.debug(f'memo.crawl: db has the valid crawl')
            if not final_url:
                return brotli.decompress(html['html']).decode()
            else:
                return brotli.decompress(html['html']).decode(), html['final_url']  
        elif html:
            try:
                self.db.crawl.update_one({'_id': url}, {'$unset': {'title': '', 'content': ''}}) 
            except: pass
        retry = 0
        resp = crawl.requests_crawl(url, raw=True, **kwargs)
        if isinstance(resp, tuple) and resp[0] is None:
            tracer.info(f'requests_crawl: Blocked url {url}, {resp[1]}')
            if not final_url:
                return None
            else:
                return None, None

        # Retry if get bad crawl
        while retry < max_retry and resp is None:
            retry += 1
            time.sleep(5)
            resp = crawl.requests_crawl(url, raw=True, **kwargs)
        if resp is None:
            tracer.info(f'requests_crawl: Unable to get HTML of {url}')
            if not final_url:
                return None
            else:
                return None, None
        html = resp.text
        if final_url:
            fu = resp.url

        # Calculate cache expire date
        headers = {k.lower(): v.lower() for k, v in resp.headers.items()}
        cache_age = DEFAULT_CACHE
        if 'cache-control' in headers:
            v = headers['cache-control']
            pp_in = 'public' in v or 'private' in v
            maxage_in = 'max-age' in v
            v = v.split(',')
            if maxage_in:
                try:
                    age = [int(vv.split('=')[1]) for vv in v if 'max-age' in vv][0]
                    cache_age = max(cache_age, age)
                except:
                    cache_age = DEFAULT_CACHE
            elif pp_in:
                cache_age = DEFAULT_CACHE
        ttl = time.time() + cache_age

        try:
            obj = {
                "_id": url,
                "url": url,
                "site": he.extract(url, wayback=is_wayback),
                "html": brotli.compress(html.encode()),
                "ttl": ttl
            }
            if final_url: obj.update({'final_url': fu})
            self.db.crawl.update_one({'_id': url}, {"$set": obj}, upsert=True)
        except Exception as e: tracer.warn(f'crawl: {url} {str(e)}')
        tracer.debug(f'memo.crawl: upsert crawl {url}')
        if not final_url:
            return html
        else:
            return html, fu
    
    def wayback_index(self, url, policy='latest-rep', ts=None, all_none_400=False, **kwargs):
        """
        Get most representative snapshot for a certain url
        all_none_400: Also crawled for 300 status code snapshots, stored as "ts_nb"
        policy: policy for getting which wayback snapshot
          - Return: wayback-form URL
            - latest-rep: Lastest representitive
            - closest: Closest to ts (ts required)
            - closest-later: closest to ts but later (ts required)
            - closest-earlier: closest to ts but earlier (ts required)
            - earliest: earliest snapshot
            - latest: latest snapshot
          - Return: [(ts, wayback-form URL)]
            - all: all snapshots (return lists instead of str)
        """
        assert(policy in {'latest-rep', 'closest-later', 'closest-earlier', 'earliest', 'latest', 'closest', 'all'})
        wayback_q = {"url": url, "policy": policy}
        if policy == 'latest-rep':
            wayback_url = self.db.wayback_rep.find_one(wayback_q)
            if wayback_url:
                return wayback_url['wayback_url']
        param_dict = {
            "filter": ['statuscode:[23][0-9]*', 'mimetype:text/html'],
            "collapse": "timestamp:8"
        }
        nb_map = {True: 'ts_nb', False: 'ts'}
        cps = self.db.wayback_index.find_one({'_id': url})
        if not cps:
            cps, status = crawl.wayback_index(url, param_dict=param_dict, total_link=True, **kwargs)
            tracer.debug('Wayback Index (tools.py): Get wayback query response')
            if len(cps) == 0: # No snapshots
                tracer.info(f"Wayback Index: No snapshots {status}")
                return None if policy not in ['all'] else []
            cps.sort(key=lambda x: x[0])
            update_dict = {
                'url': url,
                'ts': [c[0] for c in cps if str(c[2])[0] == '2'],
                'ts_nb': [c[0] for c in cps]
            }
            try:
                self.db.wayback_index.update_one({"_id": url}, {'$set': update_dict}, upsert=True)
            except: pass
            cps = update_dict
        else:
            tracer.debug('Wayback Index (tools.py): db has wayback_index')
        
        cps = [(c, url_utils.constr_wayback(url, c)) for c in cps[nb_map[all_none_400]]]
        if len(cps) == 0:
            return None if policy not in ['all'] else []

        if policy == 'closest':
            sec_diff = lambda x: (date_parse(str(x)) - date_parse(str(ts))).total_seconds()
            cps_close = [(cp, abs(sec_diff(cp[0]))) for cp in cps]
            return sorted(cps_close, key=lambda x: x[1])[0][0][1]
        elif policy == 'closest-later':
            cps_later = [cp for cp in cps if int(cp[0]) >= int(ts)]
            return cps_later[0][1] if len(cps_later) > 0 else cps[-1][1]
        elif policy == 'closest-earlier':
            cps_earlier = [cp for cp in cps if int(cp[0]) <= int(ts)]
            return cps_earlier[-1][1] if len(cps_earlier) > 0 else cps[0][1]
        elif policy == 'earliest':
            return cps[0][1]
        elif policy == 'latest':
            return cps[-1][1]
        elif policy == 'all':
            return cps
        elif policy == 'latest-rep':
            # Get latest 6 snapshots, and random sample 3 for finding representative results
            cps_sample = cps[-3:] if len(cps) >= 3 else cps
            cps_sample = [(cp[0], cp[1]) for cp in cps_sample if (date_parse(cps_sample[-1][0]) - date_parse(cp[0])).days <= 180]
            cps_dict = {}
            for ts, wayback_url in cps_sample:
                html = self.crawl(wayback_url, proxies=self.PS.select())
                if html is None: continue
                # TODO: Domditiller vs Boilerpipe --> Acc vs Speed?
                content = text_utils.extract_body(html, version='boilerpipe')
                # title = text_utils.extract_title(html, version='newspaper')
                cps_dict[ts] = (ts, wayback_url, content)
            if len(cps_dict) > 0:
                rep = sorted(cps_dict.values(), key=lambda x: len(x[2].split()))[int((len(cps_dict)-1)/2)]
            else:
                rep = cps_sample[-1]
            try:
                self.db.wayback_rep.insert_one({
                    "url": url,
                    "ts": rep[0],
                    "wayback_url": rep[1],
                    'policy': 'latest-rep'
                })
            except Exception as e: pass
            return rep[1]
        else:
            tracer.error(f'Wayback Index: Reach non existed policy')
            raise
    
    def extract_content(self, html, **kwargs):
        if html is None:
            if kwargs.get('handle_exception', True):
                return ''
            else:
                raise
        html_bin = brotli.compress(html.encode())
        try:
            content = self.db.crawl.find_one({'html': html_bin, 'content': {"$exists": True}})
        except: 
            content = None
        if content:
            return content['content']
        content = text_utils.extract_body(html, **kwargs)
        try:
            self.db.crawl.update_one({'html': html_bin}, {"$set": {'content': content}})
        except Exception as e: tracer.warn(f'extract content: {str(e)}')
        return content
    
    def extract_title(self, html, **kwargs):
        if html is None:
            if kwargs.get('handle_exception', True):
                return ''
            else:
                raise
        html_bin = brotli.compress(html.encode())
        try:
            title = self.db.crawl.find_one({'html': html_bin, 'title': {"$exists": True}})
        except: 
            title = None
        if title:
            return title['title']
        # Require to be extracted next time
        title = text_utils.extract_title(html, **kwargs)
        if title == "":
            return title
        try:
            self.db.crawl.update_one({'html': html_bin}, {"$set": {'title': title}})
        except Exception as e: tracer.warn(f'extract title: {str(e)}')
        return title
    
    def extract_title_content(self, html, **kwargs):
        if html is None:
            if kwargs.get('handle_exception', True):
                return ''
            else:
                raise
        html_bin = brotli.compress(html.encode())
        try:
            title_content = self.db.crawl.find_one({'html': html_bin, 'title': {"$exists": True}, 'content': {'$exists': True}})
        except: 
            title_content = None
        if title_content:
            return title_content['title'], title_content['content']
        # Require to be extracted next time
        title, content = text_utils.extract_title_body(html, **kwargs)
        if title == "" or content == "":
            return title, content
        try:
            self.db.crawl.update_one({'html': html_bin}, {"$set": {'title': title, 'content': content}})
        except Exception as e: tracer.warn(f'extract title content: {str(e)}')
        return title, content
    
    def get_more_crawls(self, url, html=None, year_range=None, wayback=False):
        """
        Getting more samples from the same netloc_dir with url
        Process can look for different src:
         - First look at url's html's outgoing links
         - crawl for wayback (wb with close ts, lw with recent ts)
         - search for more urls

        url: full url
        year_range: tuple, years thats want to be searched on
        Return: new crawls
        """
        tracer.debug(f'Getting more crawls from {url}')
        new_crawls = []
        seen_urls = set()
        # * Looking at outgoing links of url to see whether there are ones in the same  
        if not html:
            html = self.crawl(url)
        nd = url_utils.netloc_dir(url)

        if html:
            outlinks = crawl.outgoing_links(url, html, wayback=wayback)
            for outlink in outlinks:
                ond = url_utils.netloc_dir(outlink)
                if ond != nd or url_utils.url_match(outlink, url) or outlink in seen_urls:
                    continue
                if url_utils.na_url(outlink):
                    continue
                try:
                    out_html = self.crawl(outlink)
                    out_title = self.extract_title(out_html, version='mine', handle_exception=False)
                    out_content = self.extract_content(out_html, handle_exception=False)
                except: continue
                tracer.debug(f"get_more_crawls: Got new sample from outlinks: {outlink} {out_title}")
                new_crawl = {
                    'url': outlink,
                    'html': out_html,
                    'title': out_title,
                    'content': out_content
                }
                new_crawls.append(new_crawl)
                seen_urls.add(outlink)
                # * Gather 2 data points should be sufficient
                if len(new_crawls) > 1:
                    return new_crawls
        
        url_prefix = ''.join(nd)
        if not year_range:
            if wayback:
                year = url_utils.get_ts(url)
                year = dparser.parse(year).year
            else:
                year = 2022
            start = year -4,
            end = int(f'{year}1231')
        else:
            start, end = year_range
        # * Set year limit to avoid getting "alias" title/content as non-unique
        param = {
            'from':start,
            'to': end,
            'filter': ['mimetype:text/html', 'statuscode:200'],
            'collapse': 'urlkey',
            'limit': 100
        }
        wayback_urls, _ = crawl.wayback_index(url_prefix + '/*', param_dict=param, total_link=True)
        wayback_urls = [wu[1] for wu in wayback_urls if not url_utils.url_match(wu[1], url, wayback=wayback) and url_utils.netloc_dir(wu[1]) == nd]
        # * Also looking for its direct parent if no siblings is archived
        if len(wayback_urls) == 0:
            wayback_urls, _ = crawl.wayback_index(url_prefix, param_dict=param, total_link=True)
            wayback_urls = [wu[1] for wu in wayback_urls if not url_utils.url_match(wu[1], url, wayback=wayback)]
        if wayback:
            cand_urls = sorted(wayback_urls, key=lambda x: int(url_utils.get_ts(x)))
        else:
            cand_urls = [url_utils.filter_wayback(wu) for wu in wayback_urls]
        for cand_url in cand_urls:
            if cand_url in seen_urls or url_utils.url_match(url, cand_url):
                continue
            try:
                cand_html = self.crawl(cand_url)
                cand_title = self.extract_title(cand_html, version='mine', handle_exception=False)
                cand_content = self.extract_content(cand_html, handle_exception=False)
            except: continue
            tracer.debug(f"get_more_crawls: Got new sample from wayback")
            new_crawl = {
                'url': cand_url,
                'html': cand_html,
                'title': cand_title,
                'content': cand_content
            }
            new_crawls.append(new_crawl)
            seen_urls.add(cand_url)
            # * Gather 2 data points should be sufficient
            if len(new_crawls) > 1:
                return new_crawls
        
        # TODO: Implement search if necessary
        return new_crawls


class Similar:
    def __init__(self, use_db=True, db=None, corpus=[], threshold=0.8, short_threshold=None, corpus_size=10000):
        """
        corpus_size: size of corpus to sample on: (0-250k)
        """
        if not use_db and len(corpus) == 0:
            raise Exception("Corpus is requred for tfidf if db is not set")
        self.use_db = use_db
        self.threshold = threshold
        self.short_threshold = short_threshold if short_threshold else self.threshold - 0.1
        if use_db:
            self.db =  config.new_db() if not db else db
            corpus = self.db.corpus.aggregate([
                {'$match':  {'$or': [{'src': 'realweb'}, {'usage': re.compile('represent')}]}},
                {'$project': {'content': True}},
                {'$sample': {'size': corpus_size}},
            ], allowDiskUse=True)
            corpus = [c['content'] for c in list(corpus)]
            # corpus = random.sample(corpus, 100000)
            self.tfidf = text_utils.TFidfStatic(corpus)
        else:
            self.tfidf = text_utils.TFidfStatic(corpus)
        self.site = None
        self.separable = None

    def match_url_sig(self, old_linked_sig, new_sigs):
        """
        Calc similarities between wayback_sig and liveweb_sigs, for both anchor texts and sig texts
        Input:
            old_linked_sig: (link, anchor, sig)
            new_sigs: [(link, anchor, sig)]
        
        Return: {"anchor": {link: (link, anchor, simi)}, "sig": {link: (link, sig, simi)}}
        """
        self.tfidf._clear_workingset()
        anchor_simis, sig_simis = defaultdict(list), defaultdict(list)
        corpus = [old_linked_sig[1]] + [s for s in old_linked_sig[2] if s != '']
        # TODO (new or not?): Not consider if the liveweb still have this link (only if exact match?)
        simis = {"anchor": [], "sig": []}
        for link, anchor, sig in new_sigs:
            corpus.append(anchor)
            for s in sig:
                if s != '': corpus.append(s)
        self.tfidf.add_corpus(corpus)
        for lws in new_sigs:
            link, anchor, sig = lws
            link = url_utils.filter_wayback(link)
            simi = self.tfidf.similar(old_linked_sig[1], anchor)
            anchor_simis[link].append((link, anchor, simi))
            sig_simi = 0
            for osig in old_linked_sig[2]:
                for nsig in sig:
                    sig_simi = max(self.tfidf.similar(osig, nsig), sig_simi)
            sig_simis[link].append((link, sig, sig_simi))
        simis["anchor"] = {k: max(v, key=lambda x: x[2]) for k, v in anchor_simis.items()}
        simis["sig"] = {k: max(v, key=lambda x: x[2]) for k, v in sig_simis.items()}
        return simis

    def max_similar(self, target_content, candidates_contents, init=True):
        """
        Return the max similarity between target_content and candidates_contents
        candidates_contents: List of strings
        init: Whether clear workingset and adding corpus is required. If not, must be pre-init

        Return: (similarity, content)
        """
        assert(isinstance(candidates_contents, list))
        max_simi, max_content = 0, None
        if init:
            self.tfidf._clear_workingset()
            self.tfidf.add_corpus([target_content] + candidates_contents)
        for c in candidates_contents:
            simi = self.tfidf.similar(target_content, c)
            if simi > max_simi:
                max_simi = simi
                max_content = c
        return max_simi, max_content
    
    def _init_titles(self, site, version='domdistiller'):
        """
        Return: Bool (whether init_title is succeed)
        """
        if self.site and site in self.site:
            return True
        memo = Memoizer()
        site_urls = [f'http://{site}', f'http://www.{site}']
        for site_url in site_urls:
            _, new_site = memo.crawl(site_url, final_url=True)
            if new_site:
                break
        if new_site is None:
            return False
        new_site = he.extract(new_site)
        self.site = (site, new_site)
        tracer.info(f'_init_titles {self.site}')
        # self.lw_titles = defaultdict(set) # *{title: set(path)}
        # self.wb_titles = defaultdict(set)
        self.lw_titles = defaultdict(list) # * {title: {list(crawls)}}
        self.wb_titles = defaultdict(list)
        lw_crawl = []
        start = time.time()
        for ssite in set(self.site):
            lw_crawl += list(self.db.crawl.find({'site': ssite, 'url': re.compile('^((?!web\.archive\.org).)*$')}))
        wb_crawl = list(self.db.crawl.find({'site': site, 'url': re.compile('\/\/web.archive.org')}))
        # lw_crawl = [lw for lw in lw_crawl if 'title' in lw] + [lw for lw in lw_crawl if 'title' not in lw]
        # wb_crawl = [wb for wb in wb_crawl if 'title' in wb] + [wb for wb in wb_crawl if 'title' not in wb]
        lw_crawl = [lw for lw in lw_crawl if 'title' in lw]
        wb_crawl = [wb for wb in wb_crawl if 'title' in wb]
        # lw_path, wb_path = defaultdict(int), defaultdict(int)
        tracer.debug(f'find crawls in db: {time.time() - start:.2f}')

        self.lw_seen = set()
        start = time.time()
        # * Get more urls from search engine
        seen = set([lw['url'] for lw in lw_crawl])
        if len(lw_crawl) < LEAST_SITE_URLS:
            tracer.debug(f'_init_site: Not enough samples for liveweb page, fetching more')
            new_urls = search.bing_search(f"site:{self.site[-1]}", param_dict={'count': 50})
            iterr = 0
            while len(lw_crawl) < LEAST_SITE_URLS and iterr < len(new_urls):
                new_url = new_urls[iterr]
                iterr += 1
                if new_url in seen:
                    continue
                else:
                    seen.add(new_url)
                try:
                    html = memo.crawl(new_url)
                    content = memo.extract_content(html, handle_exception=False)
                    title = memo.extract_title(html, version='mine', handle_exception=False)
                    lw_crawl.append({
                        'site': site, 
                        '_id': new_url, 
                        'url': new_url, 
                        'html': brotli.compress(html.encode()),
                        'title': title,
                        'content': content
                    })
                except:
                    continue

        # * Guarantee every path has at lease one title
        for lw in lw_crawl:
            url = url_norm(lw['url'])
            if url in self.lw_seen: continue
            else: self.lw_seen.add(url)
            loc_dir = url_utils.netloc_dir(lw['url'])
            # if 'title' not in lw and lw_path[loc_dir] < 2:
            #     html = brotli.decompress(lw['html']).decode()
            #     title = text_utils.extract_title(html, version=version)
            #     if title == '': continue
            #     try:
            #         self.db.crawl.update_one({'_id': lw['_id']}, {"$set": {'title': title}})
            #     except: pass
            # elif 'title' in lw:
            #     title = lw['title']
            # else: continue
            # lw_path[loc_dir] += 1
            # self.lw_titles[title].add(norm(lw['url']))
    
            lw.update({'netloc_dir': loc_dir})
            self.lw_titles[lw['title']].append(lw)
        # * Prepare data structures for title prefix/suffix filteration
        # lw_crawl_title = [lw for lw in lw_crawl if 'title' in lw]
        self.lw_meta = title_prepare(lw_crawl, wayback=False)
        end = time.time()
        tracer.info(f'lw_titles: {sum([len(v) for v in self.lw_titles.values()])}, init_time: {end-start:.2f}')

        start = time.time()
        self.wb_seen = set()
        for wb in wb_crawl:
            url, wb_url = wb['url'], url_norm(url_utils.filter_wayback(wb['url']))
            if wb_url in self.wb_seen: continue
            else: self.wb_seen.add(wb_url)
            loc_dir = url_utils.netloc_dir(wb_url)
            wb_title = wb.copy()
            wb_title.update({
                'url': wb_url, 
                'ts': url_utils.get_ts(url),
                'netloc_dir': loc_dir
            })
            self.wb_titles[wb_title['title']].append(wb_title)
        # * Prepare data structures for title prefix/suffix filteration
        self.wb_meta = title_prepare(wb_crawl, wayback=True)
        end = time.time()
        tracer.info(f'wb_titles: {sum([len(v) for v in self.wb_titles.values()])} \n init_time: {end - start:.2f}')
        return True
    
    def clear_titles(self):
        self.site = None
        self.lw_titles = None
        self.wb_titles = None
        # self.lw_index = None
        self.lw_meta = None
        # self.wb_index = None
        self.wb_meta = None
        self.lw_seen = None
        self.wb_seen = None

    def _add_crawl(self, url, title, content, html=None):
        """Add new crawls into similar comparison"""
        is_wayback = 'web.archive.org/web' in url
        if is_wayback and url_norm(url_utils.filter_wayback(url)) in self.wb_seen:
            return
        elif not is_wayback and url_norm(url) in self.lw_seen:
            return
        elif not title:
            return
        if is_wayback:
            ts, url = url_utils.get_ts(url), url_utils.filter_wayback(url)
        nd = url_utils.netloc_dir(url)
        toadd = {
            'url': url,
            'html': html,
            'title': title,
            'content': content,
            'netloc_dir': nd
        }
        if is_wayback:
            toadd.update({'ts': ts})
            self.wb_titles[title].append(toadd)
            nd_idx = bisect.bisect_left(self.wb_meta, [nd, []])
            if nd_idx >= len(self.wb_meta) or self.wb_meta[nd_idx][0] != nd:
                self.wb_meta.insert(nd_idx, [nd, [toadd]])
            else:
                tss = [int(obj['ts']) for obj in self.wb_meta[nd_idx][1]]
                ts_idx = bisect.bisect_right(tss, int(ts))
                self.wb_meta[nd_idx][1].insert(ts_idx, toadd)
            self.wb_seen.add(url_norm(url))
        else:
            self.lw_titles[title].append(toadd)
            nd_idx = bisect.bisect_left(self.lw_meta, [nd, []])
            if nd_idx >= len(self.lw_meta) or self.lw_meta[nd_idx][0] != nd:
                self.lw_meta.insert(nd_idx, [nd, [toadd]])
            else:
                titles = [obj['title'] for obj in self.lw_meta[nd_idx][1]]
                title_idx = bisect.bisect_right(titles, toadd['title'])
                self.lw_meta[nd_idx][1].insert(title_idx, toadd)
            self.lw_seen.add(url_norm(url))

    def shorttext_match(self, text1, text2):
        """
        Func should only be called when self.tfidf is properly prepared
        Check whether one text is a subset of another + similar enough
        # TODO: Currently use TF-IDF for comparison, but other way may also apply

        Returns: 0 if not match. Actual similarity otherwise
        """
        text1_token = text_utils.tokenize(text1) if isinstance(text1, str) else text1
        text2_token = text_utils.tokenize(text2) if isinstance(text2, str) else text2
        # ! Choice 1: Consider sequence
        # text1_token, text2_token = ' '.join(text1_token), ' '.join(text2_token)
        # # * To match, one text must be the subset of another
        # if text1_token not in text2_token and text2_token not in text1_token:
        #     tracer.debug(f'shorttext_match: one text not a subset of another: \n{ text1} vs. {text2} \n {text1_token} vs. {text2_token}')
        #     return 0
        # ! Choice 2: No sequence
        text1_token, text2_token = set(text1_token), set(text2_token)
        # * To match, one text must be the subset of another
        if not (text1_token <= text2_token or text2_token <= text1_token):
            tracer.debug(f'shorttext_match: one text not a subset of another: "{text1}" vs. "{text2}"')
            return 0
       
        simi = self.tfidf.similar(text1, text2)
        # tracer.debug(f'shorttext_match: simi between "{text1}" vs. "{text2}": {simi}')
        return simi
        
    def _is_title_unique(self, url, title, content, wayback=False):
        """
        Check is input title is unique among the sites
        If there is no sample in the netloc_dir, use memo to get for more
        Also insert more samples into lw_meta/wb_meta

        Return: Bool
        """
        if not title:
            return False
        self._add_crawl(url, title, content)
        lw_url = url_utils.filter_wayback(url)
        site_titles = self.wb_titles if wayback else self.lw_titles
        site_meta = self.wb_meta if wayback else self.lw_meta
        nd = url_utils.netloc_dir(lw_url)
        def check_titles():
            if title in site_titles:
                for site_crawl in site_titles[title]:
                    # * title in site_titles is a child of url
                    if nd != site_crawl['netloc_dir'] and nd in site_crawl['netloc_dir']:
                        continue
                    # TODO: Can actually also compare content to not consider canonical here
                    if not url_utils.url_match(lw_url, site_crawl['url']) and \
                            text_utils.k_shingling(content, site_crawl.get('content', '')) < 0.9:
                        tracer.debug(f"_is_title_unique: title {title} is not unique amoung site with {site_crawl['url']}")
                        return False
            return True
        # unique = check_titles()
        # if not unique: return unique
        nd_idx = bisect.bisect_left(site_meta, [nd, []])
        # * Get more samples if nd's URL is not enough
        if wayback and len(site_meta[nd_idx][1]) < 2:
            memo = Memoizer()
            more_crawls = memo.get_more_crawls(url, wayback=True)
            for more_crawl in more_crawls:
                self._add_crawl(more_crawl['url'], more_crawl['title'], more_crawl['content'], more_crawl['html'])
            return check_titles()
        return True
        
    def unique_title(self, url, title, content, site_url_meta, wayback=False):
        """
        Stateful unique title: If meta needs to be updated during unique title
        
        """
        lw_url = url if not wayback else url_utils.filter_wayback(url)
        nd = url_utils.netloc_dir(lw_url)
        site_meta = self.wb_meta if wayback else self.lw_meta
        nd_idx = bisect.bisect_left(site_meta, [nd, []])
        if len(site_meta) <= nd_idx or len(site_meta[nd_idx][1]) < 2:
            memo = Memoizer()
            more_crawls = memo.get_more_crawls(url, wayback=wayback)
            for more_crawl in more_crawls:
                self._add_crawl(more_crawl['url'], more_crawl['title'], more_crawl['content'], more_crawl['html'])
        site_url_meta = self.lw_meta if not wayback else self.wb_meta
        return unique_title(url, title, content, site_url_meta, wayback)

    def title_similar(self, target_url, target_title, target_content, candidates_titles, candidates_contents, shorttext=True, fixed=True):
        """
        See whether there is UNIQUE title from candidates that is similar target
        target_url: URL in the wayback form
        candidates_x: {url: x}, with url in the same host!
        shorttext: Whether shorttext match is used

        Return: sorted([(url, similarity)], reverse=True)
        """
        global he
        site = he.extract(target_url, wayback=True)
        if site not in self.site:
            self._init_titles(site)
        if not self._is_title_unique(target_url, target_title, target_content, wayback=True):
            tracer.debug(f"title_similar: target_url's title '{target_title}' is not unique")
            return [('', 0), ('', 0)]

        self.tfidf._clear_workingset()
        # * Extract Unique Titles for both wb urls and lw urls
        tgt_uniq_title = unique_title(target_url, target_title, target_content, self.wb_meta, wayback=True)
        cand_uniq_titles = {url: unique_title(url, title, candidates_contents.get(url, ''), self.lw_meta, wayback=False) \
             for url, title in candidates_titles.items()}
        self.tfidf.add_corpus([tgt_uniq_title] + [ct for ct in cand_uniq_titles.values()])

        simi_cand = []
        for url in cand_uniq_titles:
            c, uniq_c = candidates_titles[url], cand_uniq_titles[url]
            site = he.extract(url)
            if site not in self.site and not fixed:
                self._init_titles(site)
            if not self._is_title_unique(url, c, candidates_contents.get(url, ''), wayback=False):
                tracer.debug(f"title_similar: cand_url's title '{c}' is not unique")
                continue
            if shorttext:
                simi = self.shorttext_match(tgt_uniq_title, uniq_c)
            else:
                simi = self.tfidf.similar(tgt_uniq_title, uniq_c)
            tracer.debug(f'similarity title, (value/url): ({simi}/{url})')
            simi_cand.append((url, simi))
        
        while len(simi_cand) < 2:
            simi_cand.append(("", 0))
        return sorted(simi_cand, key=lambda x: x[1], reverse=True)
    
    def content_similar(self, target_content, candidates_contents, candidates_html=None):
        """
        See whether there are content from candidates that is similar target
        candidates: {url: content}

        Return: sorted([(url, similarity)], reverse=True)
        """
        self.tfidf._clear_workingset()
        self.tfidf.add_corpus([target_content] + list(candidates_contents.values()))
        simi_cand = []
        for url, c in candidates_contents.items():
            simi = self.tfidf.similar(target_content, c)
            tracer.debug(f'similarity content, (value/url): ({simi}/{url})')
            simi_cand.append((url, simi))
        
        while len(simi_cand) < 2:
            simi_cand.append(("", 0))
        simi_cand = sorted(simi_cand, key=lambda x: x[1], reverse=True)
        # * Check if top similar URL is in its own pattern, if not, look at 
        # * This is mainly to compensate search engines' index incompleteness
        netdir_urls = defaultdict(list)
        for cand in candidates_contents: netdir_urls[url_utils.netloc_dir(cand)].append(cand)
        memo = Memoizer()
        for i in range(2):
            cand, cand_simi = simi_cand[i]
            cand_nd = url_utils.netloc_dir(cand)
            if cand and cand_simi >= self.threshold and len(netdir_urls[cand_nd]) < 2:
                if candidates_html and cand in candidates_html:
                    html = candidates_html[cand]
                    more_crawls = memo.get_more_crawls(cand, html, year_range=('20210101', '20211231'))
                    more_contents = {c['url']: c['content'] for c in more_crawls}
                    self.tfidf.add_corpus([target_content] + list(more_contents.values()))
                    for u, c in more_contents.items():
                        u_simi = self.tfidf.similar(target_content, c)
                        simi_cand.append((u, u_simi))
        
        return sorted(simi_cand, key=lambda x: x[1], reverse=True)
    
    def token_similar(self, url, target_token, candidates_tokens, shorttext=True):
        """
        For each candidate, get most similar token
        Candidates_token: {url: [tokens]}
        Return: sorted([(url, similarity, token)])
        """
        all_tokens = [target_token]
        # ? For query, only consider values
        candidates_tokens = {cand: [t.split('=')[-1] for t in tokens] for cand, tokens in candidates_tokens.items()}
        for tokens in candidates_tokens.values():
            all_tokens += tokens
        self.tfidf._clear_workingset()
        self.tfidf.add_corpus(all_tokens)
        simi_cand = []
        for can, tokens in candidates_tokens.items():
            max_token = (can, 0, '')
            for t in tokens:
                if shorttext:
                    simi = self.shorttext_match(target_token, t)
                else:
                    simi = self.tfidf.similar(target_token, t)
                tracer.debug(f'similarity title, (value/url): ({simi}/{target_token} vs. {t})')
                if simi > max_token[1]:
                    max_token = (can, simi, t)
            simi_cand.append(max_token)
        while len(simi_cand) < 2:
            simi_cand.append(("", 0, ""))
        return sorted(simi_cand, key=lambda x: x[1], reverse=True)
    
    def _separable(self, simi, threshold=None):
        threshold = self.threshold if not threshold else threshold
        return simi[0][-1] >= threshold and simi[1][-1] < threshold

    def similar(self, tg_url, tg_title, tg_content, cand_titles, cand_contents, \
                cand_htmls={}, fixed=True, match_order=['title', 'content'], **kwargs):
        """
        All text-based similar tech is included
        Fixed: Whether title similarity is allowed across different sites
        matched_order: how different types of match should be ordered

        Return: [((similar urls, similarity), from which comparison(title/content))], if there is some similarity
                else: [(None, from)]
        """
        self._add_crawl(tg_url, tg_title, tg_content)
        if not self.separable:
            separable = lambda x: x[0][1] >= self.threshold and x[1][1] < self.threshold
        else:
            separable = self.separable
        matched_alias = []
        for cand_url in cand_titles:
            self._add_crawl(cand_url, cand_titles[cand_url], cand_contents.get(cand_url), \
                            cand_htmls.get(cand_url))
        if self.site is not None and tg_title:
            title_similars = self.title_similar(tg_url, tg_title, tg_content, cand_titles, cand_contents, fixed=fixed, **kwargs)
            if separable(title_similars):
                matched_alias.append((title_similars[0], "title"))
                # return title_similars, "title"
        content_similars = self.content_similar(tg_content, cand_contents, cand_htmls)
        if separable(content_similars):
            matched_alias.append((content_similars[0], "content"))
            # return content_similars, "content"
        # * matched_alias: [((url, simi), "content"/"title")]
        get_order = lambda x: -match_order.index(x)
        tracer.debug(f'matched_alias: {matched_alias}')
        if len(matched_alias) > 0:
            matched_alias.sort(reverse=True, key=lambda x: (get_order(x[1]), x[0][1]))
            return matched_alias    
        else:
            return [(None, "")]


def is_canonical(url1, url2, resp1=None, resp2=None):
    """
    Decide whether two URLs are canonical of each other
    Both url1 and url2 should be good pages
    Check with metrics:
    1. URLs' final urls are the same
    2. URLs' response canonical are the same
    3. Responses are indifferentiable

    Return: (currently) boolean on if is canonical
    """
    if url_utils.url_match(url1, url2):
        return True
    if not resp1:
        resp1 = crawl.requests_crawl(url1, raw=True, timeout=10)
    if not resp2:
        resp2 = crawl.requests_crawl(url2, raw=True, timeout=10)
    if isinstance(resp1, tuple) or not resp1:
        return False
    if isinstance(resp2, tuple) or not resp2:
        return False
    # * Check final url
    if url_utils.url_match(resp1.url, resp2.url):
        return True
    # * Check canonical
    try:
        soup1 = BeautifulSoup(resp1.text, 'lxml')
        soup2 = BeautifulSoup(resp2.text, 'lxml')
        cans1 = soup1.find_all('link', {'rel': 'canonical'})
        cans2 = soup2.find_all('link', {'rel': 'canonical'})
        can1, can2 = '', ''
        if len(cans1) > 0:
            if urlsplit(resp1.url).path not in ['', '/']:
                can1 = cans1[0]['href']
        if len(cans1) > 0:
            if urlsplit(resp2.url).path not in ['', '/']:
                can2 = cans2[0]['href']
        # * Check for potential match
        if can1 and can2 and url_utils.url_match(can1, can2):
            return True
        if can1 and url_utils.url_match(can1, resp2.url):
            return True
        if can2 and url_utils.url_match(can2, resp1.url):
            return True
    except:
        pass
    try:
        content1 = soup1.get_text(separator=' ')
    except:
        content1 = resp1.text
    try:
        content2 = soup2.get_text(separator=' ')
    except:
        content2 = resp2.text
    if text_utils.k_shingling(text_norm(content1), text_norm(content2)) >= 0.95:
        return True
    return False

def get_unique_token(url, fuzzy=False):
    """
    Given a URL, return which tokens should be put into search engine
    fuzzy: Get fuzzy with extraction of tokens
    """
    us = urlsplit(url)
    path = us.path
    if path == '': path = '/'
    if path[-1] == '/' and path != '/': path = path[:-1]
    path = path.split('/')
    query = parse_qsl(us.query)
    if 'index' in path[-1]: path = path[:-1]
    available_tokens = []
    params = {
        'output': 'json',
        "limit": 10000,
        'collapse': 'urlkey',
        'filter': ['statuscode:[23][0-9]*', 'mimetype:text/html'],
    }
    def _collapse_index(li):
        urls = set()
        for url in li:
            us = urlsplit(url)
            path = us.path
            if path == '': path = '/'
            if path[-1] == '/' and path != '/': path = path[:-1]
            path = us.path.split('/')
            if 'index' in path[-1]: path = path[:-1]
            url = urlunsplit(us._replace(path='/'.join(path)))
            urls.add(url)
        return urls
    def _is_id(s):
        if s.isdigit():
            s = int(s)
            return s > 2050 # ? Not year
    def _unique_query(li, query):
        qs = [v[1] for v in query]
        q_count = defaultdict(int)
        for _, v in query:
            q_count[v] += 1
        for url in li:
            us = urlsplit(url)
            liq = parse_qsl(us.query)
            for _, v in liq:
                q_count[v] += 1
        return [q for q in qs if q_count.get(q) <= 1 or _is_id(q)]
    def _split_token(s):
        """Split a token into id and the remaining"""
        splitted = []
        st = regex.split("[^a-zA-Z0-9]", s)
        if len(st) < 0:
            return s
        start, end = 0,len(st)
        if _is_id(st[0]): 
            splitted.append(st[0])
            start+=1
        if _is_id(st[-1]): 
            splitted.append(st[-1])
            end-=1
        st = st[start:end]
        splitted.append(' '.join(st).strip())
        return splitted
    def _good_token(s):
        """Token not differentiable if too short"""
        if not _is_id(s) and len(regex.split("[^a-zA-Z0-9]", s)) <= 1:
            if len(s) <= 5:
                return False
        return True
    # * Check for unique token in the path
    for i in range(len(path)-1, 0, -1):
        sub_path = '/'.join(path[:i+1])
        sub_us = us._replace(path=sub_path + '*', query='', fragment='')
        sub_url = urlunsplit(sub_us)
        wayback_index, _ = crawl.wayback_index(sub_url, param_dict=params)
        # print(sub_url)
        wayback_index = _collapse_index([w[1] for w in wayback_index])
        tracer.debug(f'get_unique_token: {sub_url}, {len(wayback_index)}')
        if len(query):
            available_tokens += _unique_query(wayback_index, query)
        if len(wayback_index) <= 1:
            available_tokens  += _split_token(path[i])
        else:
            break
    available_tokens = list(set(available_tokens))
    available_tokens = [a for a in available_tokens if _good_token(a)]
    available_tokens.sort(reverse=True, key=lambda x: len(x))
    return available_tokens
    
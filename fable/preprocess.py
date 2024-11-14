"""
Preprocess the URLs to select ones that are possible to find aliases
Mainly:
0. URL clearance
1. Site needs to be working
2. (Optional) site in English
"""
import json
import sys
import requests
import os
sys.setrecursionlimit(1500)

from .utils import url_utils, crawl, text_utils
from collections import defaultdict, OrderedDict
from urllib.parse import urlsplit, parse_qsl, urlunsplit
from concurrent import futures


def _apply_filter_sites(netloc_urls, filter_sites):
    """Filter certain sites"""
    new_netloc_urls = defaultdict(list)
    for netloc, urls in netloc_urls.items():
        site = url_utils.he.extract(f'http://{netloc}')
        if site not in filter_sites:
            new_netloc_urls[netloc] = urls
    return new_netloc_urls


def _apply_filter_url(netloc_urls):
    """
    Filter certain URLs or their queries
    Filter login pages, signin pages
    """
    print("Filter URL before", len(netloc_urls), sum(len(v) for v in netloc_urls.values()))
    nonhtml_ext = {".jpg", ".gif", ".png", ".pdf", ".txt", ".js", ".css",\
                    ".json", ".start", ".xml", ".jpeg", ".svg", ".dbml", \
                    ".ico", ".doc", ".mp4", ".docx", ".exe", ".zip" }
    keywords = ['signin', 'login']
    new_netloc_urls = defaultdict(set)
    for netloc, urls in netloc_urls.items():
        for url in urls:
            us = urlsplit(url)
            _, ext = os.path.splitext(us.path)
            if ext in nonhtml_ext: 
                continue
            qsl = [f'{k}={v}' for k, v in parse_qsl(us.query) if 'utm' not in k]
            newquery = '&'.join(qsl) if len(qsl) else us.query
            us = us._replace(query=newquery, fragment="")
            url = urlunsplit(us)
            seen = False
            for k in keywords:
                if k in url:
                    seen = True
            if not seen:
                new_netloc_urls[netloc].add(url)
    new_netloc_urls = {k: list(v) for k, v in new_netloc_urls.items()}
    print("Filter URL after", len(new_netloc_urls), sum(len(v) for v in new_netloc_urls.values()))
    return new_netloc_urls

def get_sites(netloc_urls):
    all_sites = set()
    for netloc in netloc_urls:
        site = url_utils.he.extract(f'http://{netloc}')
        all_sites.add(site)
    return list(all_sites)


def gen_sites_info(sites, num_workers=1, clear=False):
    """
    Generate {site: info} as: 
    {language: str, final_site: str}
    language: lan if site works, otherwise 'No html'/'Not allowed html'
    
    clear: Start from the very beginning 
    """
    if clear or not os.path.exists('.site_language.json'):
        json.dump({}, open('.site_language.json', 'w+'), indent=2)
    site_lan = json.load(open('.site_language.json', 'r'))
    
    to_detect = [site for site in sites if site not in site_lan or site_lan[site] is None]
    print("Total sites to crawl:", len(to_detect))


    def sitelan(html):
        if html is None:
            return "No html"
        elif isinstance(html, tuple):
            return "Not allowed html"
        else:
            try:
                lan = text_utils.detect_lan(html, fuzzy=True)
                return lan
            except Exception as e:
                print(str(e))
                return None

    def worker(i, site, site_url):
        he = url_utils.HostExtractor()
        html = crawl.requests_crawl(site_url, raw=True)
        new_site = he.extract(html.url) if isinstance(html, requests.Response) else site
        lan = sitelan(html)
        print(i, site, lan)
        return lan, new_site

    with futures.ThreadPoolExecutor(max_workers=num_workers) as e:
        fs = OrderedDict()
        for i, site in enumerate(to_detect):
            site_url = f'http://{site}'
            fs[site] = e.submit(worker, i, site, site_url)
        
        print(len(fs))
        for i, (site, lan) in enumerate(fs.items()):
            r = lan.result()
            site_lan[site] = {'language': r[0], 'final_site': r[1]}
            if i % 50 == 0:
                json.dump(site_lan, open('.site_language.json', 'w+'), indent=2)
    json.dump(site_lan, open('.site_language.json', 'w+'), indent=2)
    return site_lan


def urls_working_site(netloc_urls, language_set=None, site_map=None):
    """Join URLs with working (English) site"""
    print("Working site before", len(netloc_urls), sum(len(v) for v in netloc_urls.values()))
    new_netloc_urls = defaultdict(list)
    for netloc, urls in netloc_urls.items():
        site = url_utils.he.extract(f"http://{netloc}")
        lan = site_map.get(site, {'language': 'No html'})
        if lan['language'] in {'No html', 'Not allowed html'}:
            continue
        if language_set and lan['language'] not in language_set:
            continue
        new_netloc_urls[netloc] = urls
    print("Working site after", len(new_netloc_urls), sum(len(v) for v in new_netloc_urls.values()))
    return new_netloc_urls


def preprocess(urls, filter_sites=None):
    """Default order of preprocessing URLs"""
    netloc_urls = defaultdict(list)
    for url in urls:
        nd = url_utils.netloc_dir(url, exclude_index=True)
        netloc_urls[nd[0]+nd[1]].append(url)
    if filter_sites:
        netloc_urls = _apply_filter_sites(netloc_urls, filter_sites)
    netloc_urls = _apply_filter_url(netloc_urls)
    sites = get_sites(netloc_urls)
    site_map = gen_sites_info(sites, num_workers=30)
    netloc_urls = urls_working_site(netloc_urls, language_set={"en", "de", "fr"}, site_map=site_map)
    return netloc_urls
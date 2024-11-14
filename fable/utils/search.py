"""
Utils library for search
"""
import requests
import json
import sys, time
from bs4 import BeautifulSoup
from pymongo import MongoClient


from fable import config
from . import text_utils, crawl, url_utils


requests_header = {'user-agent': config.config('user_agent')}
headers = {"Ocp-Apim-Subscription-Key": config.BING_SEARCH_KEY}
bypass_proxy = {
    'http': None,
    'https': None
}

google_url = 'https://www.googleapis.com/customsearch/v1'
bing_url = 'https://api.bing.microsoft.com/v7.0/search'

host_extractor = url_utils.HostExtractor()

def get_headers(html):
    soup = BeautifulSoup(html, 'lxml')
    possible = []
    title = soup.find('title')
    title = title.text if title and title.title != 'Wayback Machine' else ""
    for i in range(1, 7):
        tags = soup.find_all('h' + str(i))
        for tag in tags:
            if tag.text != "" and "Wayback Machine" not in tag.text: 
                if tag.text in title:               
                    return tag.text
                else:
                    possible.append(tag.text)
    return possible[0] if len(possible) > 0 else ""


def get_title(html):
    """
    Wrappers for getting decent title of a page
    """
    if html is None:
        return ''
    versions = ['domdistiller', 'newspaper']
    for v in versions:
        try:
            title = text_utils.extract_title(html, version=v)
            # print(title)
            assert(title != "")
            return title
        except: pass
    return get_headers(html)


def google_search(query, end=0, param_dict={}, site_spec_url=None, use_db=False):
    """
    Search using google
    If get 403, return None
    site_spec_url: If set, will only search within the site
    use_db: If set, will query db before calling API, and update results to db
    """
    google_query_dict = {
        "q": None,
        "key" : config.GOOGLE_SEARCH_KEY,
        "cx" : config.GOOGLE_SEARCH_CX
    }
    google_query_dict['q'] = query
    if site_spec_url:
        try:
            if '://' not in site_spec_url: site_spec_url = f'http://{site_spec_url}'
            r = requests.get(site_spec_url, headers=crawl.requests_header, timeout=10, proxies=bypass_proxy)
            site = host_extractor.extract(r.url)
            param_dict.update({'siteSearch': site})
        except: site = ""
    else: site = ""
    google_query_dict.update(param_dict)
    count = 0
    if use_db:
        db = config.DB
        result = db.searched.find_one({'query': query, 'site': site, 'engine': 'google'})
        if result is not None:
            # print("Search hit on db")
            return result['results']
    while True:
        try:
            r = requests.get(google_url, params=google_query_dict, proxies=bypass_proxy)
            status_code = r.status_code
            r = r.json()
        except Exception as e:
            print(str(e))
            return []
        if "items" not in r:
            # print(r, status_code)
            if status_code != 403:
                time.sleep(1)
                if use_db:
                    db.searched.insert_one({'query': query, 'site': site, 'engine': 'google', 'results': []})
                return []
            elif count < 3: 
                count += 1
                time.sleep(1)
                continue
            else:
                json.dump(r, open('search_err.json', 'w+'))
                return None
        end = len(r['items']) if end == 0 else min(len(r["items"]), end)
        results = [ u["link"] for u in r['items'][:end]]
        if use_db:
            db.searched.insert_one({'query': query, 'site': site, 'engine': 'google', 'results': results})
        time.sleep(1)
        return results


def bing_search(query, end=0, param_dict={}, site_spec_url=None, use_db=False):
    """
    Search using bing
    """
    bing_query_dict = {
        "q": None
    }
    bing_query_dict["q"] = query
    bing_query_dict.update(param_dict)
    count = 0
    if use_db:
        db = config.DB
        result = db.searched.find_one({'query': query, 'engine': 'bing'})
        if result is not None:
            # print("Search hit on db")
            return result['results']
    try:
        r = requests.get(bing_url, params=bing_query_dict, headers=headers, proxies=bypass_proxy)
        r = r.json()
    except Exception as e:
        print(str(e))
        time.sleep(1)
        return []
    if "webPages" not in r or 'value' not in r['webPages']:
        if use_db:
         db.searched.insert_one({'query': query, 'engine': 'bing', 'results': []})
        time.sleep(1)
        return []
    values = r["webPages"]['value']
    end = len(values) if end == 0 else min(len(values), end)
    results = [u['url'] for u in values[:end]]
    if use_db:
        db.searched.insert_one({'query': query, 'engine': 'bing', 'results': results})
    time.sleep(1)
    return [u['url'] for u in values[:end]]

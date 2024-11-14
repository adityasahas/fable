"""
Test all three techniques at once
"""
from fable.Archive import discoverer2
import pytest
import logging
import os
import json
import threading

from fable import tools, searcher, inferer, tracer, config
from fable.utils import url_utils

he = url_utils.HostExtractor()
memo = tools.Memoizer()
db = config.DB
simi = None
ifr = None
tr = None
simi = None
simi2 = None

db = config.DB
srh = None
dis = None
ifr = None
tr = None

def _init_large_obj():
    global simi, srh, dis, ifr, tr
    if tr is None:
        try:
            os.remove(os.path.basename(__file__).split(".")[0] + '.log')
        except: pass
        logging.setLoggerClass(tracer.tracer)
        tr = logging.getLogger('logger')
        logging.setLoggerClass(logging.Logger)
        tr._unset_meta()
        tr._set_meta(os.path.basename(__file__).split(".")[0], db=db, loglevel=logging.DEBUG)
    if simi is None:
        simi = tools.Similar()
        simi2 = tools.Similar(threshold=0.7)
    if srh is None:
        srh = searcher.Searcher(memo=memo, similar=simi)
        dis = discoverer2.Discoverer(memo=memo, similar=simi2)
        ifr = inferer.Inferer(memo=memo, similar=simi)

results = {
    'search': None,
    'backlink': None,
    'inference': None
}

def _search(url, srh):
    alias = srh.search(url, search_engine='bing')
    if alias is None:
        alias = srh.search(url, search_engine='google')
    results['search'] = alias

def _backlink(url, dis):
    alias = dis.discover(url)
    results['backlink'] = alias

def _inference(urlmeta, examples, site, ifr):
    ifr.init_site(site)
    poss_infer = ifr.infer(examples, urlmeta)
    poss_infer = ifr._filter_multicast(poss_infer)
    print("possible infer:", poss_infer)
    alias = None
    for url, poss_aliases in poss_infer.items():
        alias, _ = ifr._verify_alias(url, poss_aliases)
    results['inference'] = alias

def test_composite_once():
    _init_large_obj()
    data = {
        'url': 'http://support.apple.com/kb/HT5467',
        'example_file': ''
    }
    toinfer = False
    try:
        obj = json.load(open('examples/'+data['example_file'], 'r'))
        urlmeta = obj['urls']
        examples = obj['examples']
        toinfer = True
    except: pass
    url = data['url']
    site = he.extract(url)
    srh.similar._init_titles(site)
    dis.similar._init_titles(site)
    threads = []
    threads.append(threading.Thread(target=_search, args=(url, srh,)))
    threads[-1].start()
    threads.append(threading.Thread(target=_backlink, args=(url, dis,)))
    threads[-1].start()
    if toinfer:
        threads.append(threading.Thread(target=_inference, args=(urlmeta, examples, site, ifr,)))
        threads[-1].start()
    for t in threads:
        t.join()
    print(results)

test_composite_once()
from fable.Archive import discoverer, discoverer2
import pytest
import logging
import os

from fable import tools, tracer, config
from fable.utils import url_utils

he = url_utils.HostExtractor()
memo = tools.Memoizer()
simi = None
db = config.DB
dis = None
tr = None

def _init_large_obj():
    global simi, dis, tr
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
        simi = tools.Similar(threshold=0.7)
    if dis is None:
        dis = discoverer2.Discoverer(memo=memo, similar=simi)

def test_backlink_withalias():
    """URLs that should be found alias by backlink"""
    _init_large_obj()
    url_alias = [
        ("http://www.hubspot.com:80/company-news/author/Juliette%20Kopecky", "http://www.hubspot.com:80/company-news/author/Juliette-Kopecky"),
        ("http://www.byui.edu:80/automotive-technology/vehicle-repair", "https://www.byui.edu/program/automotive/vehicle-repairs"),
        ("http://www.consumerreports.org:80/cro/appliances/kitchen-appliances/coffeemakers/pod-coffeemaker-ratings/models/price-and-shop/buy-keurig-k45-elite-brewing-system-99048951.htm", 
            "")
    ]
    for url, alias in url_alias:
        print(url)
        site = he.extract(url)
        dis.similar._init_titles(site)
        alias = dis.discover(url)
        assert(alias[0] is not None)


def test_backlink_noalias():
    """URLs that should not be found alias by backlink"""
    _init_large_obj()
    urls = [
        "http://www.icann.org:80/en/tlds/agreements/name/registry-agmt-appc-03jul01.htm",
        "https://www.maxmind.com/de/company/contact-us"
    ]
    for url in urls:
        print(url)
        site = he.extract(url)
        dis.similar._init_titles(site)
        alias = dis.discover(url)
        assert(alias[0] is None)

unsolved = {
    # ! Correct backlink page not ranked at top and get cut off
    "http://www.hubspot.com:80/company-news/author/Juliette%20Kopecky": True,
    "https://www.edx.org/node/1022": True,    
    "http://www.atlassian.com:80/company/customers/case-studies/nasa": True
}

def test_backlink_temp():
    """Temporary test to avoid long waiting for other tests"""
    _init_large_obj()
    urls = [
        "http://www.ucsf.edu/alumni_friends/index.html"
    ]
    for url in urls:
        site = he.extract(url)
        dis.similar._init_titles(site)
        alias = dis.discover(url)
        tr.info(f'alias: {alias}')
        assert(alias[0] is not None)
import pytest
import logging
import os

from fable import tools, tracer, config
from fable.utils import url_utils, sic_transit

he = url_utils.HostExtractor()
db = config.DB

unsolved = {
  "http://www.shopify.com/enterprise/44340803-3-common-misconceptions-about-conversion-rate-optimization-that-are-wasting-your-time?ad_signup=true&utm_source=cio&utm_medium=email&utm_campaign=digest_post_16d&utm_content=email_18"\
      : False  # ! Only previously existed URLs will redirect to the homepage
}

def test_sictransit_isbroken():
    """URLs that should be broken"""
    urls = [
        "https://careers.unilever.com/job/north-rocks/assistant-procurement-manager/34155/19983463008",
        "https://www.vevo.com/watch/k-ci-and-jojo/you-bring-me-up-remix/USUV70601491",
        "https://www.dartmouth.edu/wellness/new-location.html",
        "https://www.att.com/es-us/accessories/specialty-items/gopro-gooseneck-mount-all-gopro-cameras.html",
        "http://antigua.impacthub.net/es/"
    ]
    for i, url in enumerate(urls):
        print(i, url)
        broken, _ = sic_transit.broken(url, html=True)
        assert(broken == True)

def test_sictransit_notbroken():
    """URLs that should not be broken"""
    urls = [
        "https://developer.chrome.com/extensions/contentSecurityPolicy.html",
        "https://www.jesmine.com.au/collections/summer-new-arrivals/products/royal-floral-elephant-bedding-set",
        "http://ionicframework.com/docs/api/directive/ionNavView/",
        "http://www.tennisfame.com/hall-of-famers/inductees/maud-barger-wallach"
    ]
    for url in urls:
        print(url)
        broken, _ = sic_transit.broken(url, html=True)
        assert(broken == False)
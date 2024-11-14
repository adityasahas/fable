import pytest
import logging
import os

from fable import tools, histredirector, tracer, config
from fable.utils import url_utils
import time, json

he = url_utils.HostExtractor()
memo = tools.Memoizer()
db = config.DB
hist = None
tr = None

def _init_large_obj():
    global hist, tr
    if tr is None:
        try:
            os.remove(os.path.basename(__file__).split(".")[0] + '.log')
        except: pass
        logging.setLoggerClass(tracer.tracer)
        tr = logging.getLogger('logger')
        logging.setLoggerClass(logging.Logger)
        tr._unset_meta()
        tr._set_meta(os.path.basename(__file__).split(".")[0], db=db, loglevel=logging.DEBUG)
    if hist is None:
        hist = histredirector.HistRedirector(memo=memo)

def test_waybackalias_withalias():
    """URLs that should be found alias by wayback_alias"""
    _init_large_obj()
    url_alias = [
        ("http://www.bbc.co.uk:80/1xtra/djs/rampage/", "http://www.bbc.co.uk/1xtra/rampage/"),
        ("http://www.atlassian.com:80/company/customers/case-studies/nasa", "https://www.atlassian.com/customers/nasa"),
        ("https://www.docusign.com/esignature/electronically-sign", "https://www.docusign.com/products/electronic-signature"),
        ("http://www.starcitygames.com:80/magic/ravlimited/11682-The-Weekly-Guild-Build-What-About-Bob.html", "http://www.starcitygames.com/magic/ravlimited/11682_The_Weekly_Guild_Build_What_About_Bob.html")
    ]
    for url, alias in url_alias:
        print(url)
        alias = hist.wayback_alias(url)
        assert(alias is not None)

def test_waybackalias_noalias():
    """URLs that should not be found alias by wayback_alias"""
    _init_large_obj()
    urls = [
        "http://www.intel.com:80/cd/corporate/europe/emea/eng/belgium/358249.htm",
        "https://www.meetup.com/1-Startup-Vitoria/messages/boards/forum/16297542/?sort=ThreadReplyCount&order=DESC",
        "http://www.att.com/accessories/es/specialty-items/gopro-gooseneck-mount-all-gopro-cameras.html?locale=es_US",
        "https://www.att.com/audio/ua-bluetooth-wireless-headphones-engineered-by-jbl.html",
        "http://www.skype.com:80/company/legal/terms/etiquette.html",
        "http://www.mediafire.com/?32qrp1eht670iiu",
        "http://www.dartmouth.edu:80/wellness/get_help/anthem_nurseline.html",
        "http://www.bbc.co.uk/5live/programmes/genres/sport/formulaone/current",
        "http://www.rollingstone.com:80/artists/default.asp?oid=2228",
        "http://www.forrester.com/rb/search/results.jsp?SortType=Date&nb=1&dAg=10000&N=50117+133001+50662",
        "http://www.forrester.com/rb/search/results.jsp?SortType=Date&nb=1&dAg=10000&N=50060+133001+12662",
        "http://www.onjava.com:80/pub/a/onjava/2003/11/19/filters.html?page=1",
        "http://www.ubc.ca/okanagan/vod/?f=http://cdn.ok.ubc.ca/_ubc_clf/_clf7_assets/video/I2.flv",
        "http://www.technologyreview.com:80/articles/04/11/talbot1104.asp?p=2",
        "http://www.airbnb.com:80/manhattan/monthly-houses",
        "http://cms.clevelandclinic.org:80/breastcenter/default.cfm?oTopID=108&source=breastcenterurl"
    ]
    for url in urls:
        print(url)
        alias = hist.wayback_alias(url)
        assert(alias is None)

unsolved = {
    "http://www.shopify.com:80/blog/15964292-3-common-misconceptions-about-conversion-rate-optimization-that-are-wasting-your-time?ad_signup=true&utm_source=cio&utm_medium=email&utm_campaign=digest_post_16d&utm_content=email_18": False, 
    ("https://www.cloudera.com/content/cloudera-content/cloudera-docs/CDH5/latest/CDH5-Security-Guide/cdh5sg_yarn_container_exec_errors.html", "http://www.cloudera.com/content/www/en-us/documentation/enterprise/latest/topics/cdh_sg_yarn_container_exec_errors.html"): 
        True,

    # ! All other neighors are not broken except this one
    "http://www.hollywood.com/?p=60227863": False,
    "http://econsultancy.com:80/ca/events/the-top-8-trends-in-social-media-marketing-opportunities-for-2014": False,

    # ! Suspicious from ground truth
    "http://www.mckinsey.com:80/careers/join_us/university_recruiting/internships_at_mckinsey": False,
    "http://aspn.activestate.com:80/ASPN/Cookbook/Python/Recipe/511508": False, # ! Only this one redirects to parent page
    "http://www.commerce.gov/blog/2013/11/06/nist-issues-new-standard-handheld-chemical-detectors-aid-first-responders": False, # ! Only this one redirects to parent page
    "http://careers.jpmorgan.com:80/careers/programs/research-fulltime-analyst": False, # ? Merged/Renamed to another page?
    "http://www.developer.com/feedback/ws/android/development-tools/the-9-most-anticipated-features-in-android-gingerbread-2.3.html": False, # ? 200 after 300
    
    "https://www.docusign.com/esignature/electronically-sign": False # ! Found another neighbor with the same redirected URL (only from inferback)
}

def test_waybackalias_temp():
    """Temporary test to avoid long waiting for other tests"""
    _init_large_obj()
    urls = [
        "http://www.iucnredlist.org/search/details.php/32941/all"
    ]
    for url in urls:
        print(url)
        alias = hist.wayback_alias(url)
        assert(alias is None)

def test_waybackalias_hist_temp():
    """Temporary test to avoid long waiting for other tests"""
    _init_large_obj()
    urls = [
        "https://query.nytimes.com/gst/fullpage.html?res=9c01e3df1231f931a35756c0a9649c8b63"
    ]
    for url in urls:
        print(url)
        alias = hist.wayback_alias_any_history(url)
        print(alias)
        assert(alias is None)

def test_waybackalias_batch_temp():
    _init_large_obj()
    start = time.time()
    urls = [
        "http://www.world-heritage-tour.org/visitSite.php?siteID=1208",
        "http://www.world-heritage-tour.org/visitSite.php?siteID=912",
        "http://www.world-heritage-tour.org/visitSite.php?siteID=340",
        "http://www.world-heritage-tour.org/visitSite.php?siteID=779",
        "http://www.world-heritage-tour.org/visitSite.php?siteID=1003",
        "http://www.world-heritage-tour.org/visitSite.php?siteID=778",
        "http://www.world-heritage-tour.org/visitSite.php?siteID=1077",
        "http://www.world-heritage-tour.org/visitSite.php?siteID=113"
    ]
    # for url in urls:
    #     results = hist.wayback_alias_history(url)
    results = hist.wayback_alias_batch_history(urls)
    print(json.dumps(results, indent=2))
    end = time.time()
    print(end - start)

test_waybackalias_hist_temp()
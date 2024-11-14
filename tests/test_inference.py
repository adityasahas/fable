import pytest
import logging
import os
import json

from fable import tools, inferer, tracer, config
from fable.utils import url_utils

he = url_utils.HostExtractor()
memo = tools.Memoizer()
db = config.DB
simi = None
ifr = None
tr = None

def _init_large_obj():
    global simi, ifr, tr
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
    if ifr is None:
        ifr = inferer.Inferer(memo=memo, similar=simi)

def test_inferer_temp():
    """Temporary test to avoid long waiting for other tests"""
    _init_large_obj()
    examples_urls = [
        "ubc.ca_0.json"
    ]
    for obj_file in examples_urls:
        obj = json.load(open('examples/'+obj_file, 'r'))
        examples = obj['examples']
        urls = obj['urls']
        url = obj['urls'][0][0]
        print(url)
        site = he.extract(url)
        ifr.init_site(site)
        ifr.similar._init_titles(site)
        examples_list = ifr.cluster_examples(examples)
        alias = None
        for examples in examples_list:
            poss_infer = ifr.infer(examples, urls)
            poss_infer = ifr._filter_multicast(examples, poss_infer)
            print("possible infer:", poss_infer)
            for url, poss_aliases in poss_infer.items():
                alias, _ = ifr._verify_alias(url, poss_aliases)
                print("alias:", alias)
            if alias is not None:
                break
        assert(alias is not None)

test_inferer_temp()
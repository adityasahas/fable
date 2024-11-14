"""
Test all three techniques at once
"""
import pytest
import logging
import os
import json
import threading

from fable import tools, neighboralias, tracer, config
from fable.utils import url_utils

db = config.DB
nba = None
tr = None

def _init_large_obj():
    global tr, nba
    if tr is None:
        try:
            os.remove(os.path.basename(__file__).split(".")[0] + '.log')
        except: pass
        logging.setLoggerClass(tracer.tracer)
        tr = logging.getLogger('logger')
        logging.setLoggerClass(logging.Logger)
        tr._unset_meta()
        tr._set_meta(os.path.basename(__file__).split(".")[0], db=db, loglevel=logging.DEBUG)
    if nba is None:
        nba = neighboralias.NeighborAlias()

def test_get_neighbors():
    _init_large_obj()
    urls = [
        "http://www.metacritic.com/movie/ghostworld",
        "http://www.metacritic.com/movie/meangirls",
        "http://www.metacritic.com/movie/spiderman-3",
        "http://www.metacritic.com/movie/hail-caesar",
        "https://www.metacritic.com/movie/Zack-and-Meri-Make-a-Porno",
        "http://www.metacritic.com/movie/27dresses",
        "http://www.metacritic.com/movie/anacondasthehuntforthebloodorchid",
        "https://www.metacritic.com/movie/eyes-wide-open-",
        "http://www.metacritic.com/movie/the-water",
        "http://www.metacritic.com/movie/monalisasmile",
        "https://www.metacritic.com/movie/%D5%A5%D6%80%D5%AF%D6%80%D5%BA%D5%A1%D5%A3%D5%B8%D6%82%D5%B6%D5%A5%D6%80",
        "http://www.metacritic.com/movie/journeytothecenteroftheearth",
        "http://www.metacritic.com/movie/punisher2",
        "http://www.metacritic.com/movie/kiterunner",
        "http://www.metacritic.com/movie/three-colors-rede",
        "http://www.metacritic.com/movie/pirates-of-the-caribbean-dead-men-tell-non-tales"
  ]
    neighbors = nba.get_neighbors(urls, status_filter='2')
    print(len(neighbors))
    print(json.dumps(neighbors[:min(len(neighbors), 20)], indent=2))


def test_neighbor_aliases():
    _init_large_obj()
    urls = [
        "http://pc.ign.com/articles/935/935119p2.html",
        "http://pc.ign.com/articles/159/159942p1.html",
        "http://pc.ign.com/articles/101/1011624p1.html",
        "http://pc.ign.com/articles/880/880936p1.html",
        "http://pc.ign.com/articles/759/759391p1.html",
        "http://pc.ign.com/articles/961/961510p1.html",
        "http://pc.ign.com/articles/745/745105p1.html",
        "http://pc.ign.com/articles/948/948555p2.html",
        "http://pc.ign.com/articles/876/876701p1.html",
    ]
    sheet = nba.neighbor_aliases(urls, spec_method=['search_fuzzy', 'backlink_basic'], status_filter='2', max_trials=5)
    print(json.dumps(sheet, indent=2))

# test_get_neighbors()
test_neighbor_aliases()
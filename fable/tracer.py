"""
Global tracer for recording the metadata gathered during finding aliases
"""
import re, os, sys
from collections import defaultdict
import logging
import inspect

from . import config

db = config.DB

default_name = 'fable'

class tracer(logging.Logger):
    def __init__(self, name=default_name, db=db):
        """
        name: name of the trace
        update_data: {url: update data} for updating the database
        """
        logging.Logger.__init__(self, name)
        self.name = name
        self.db = db
        self.update_data = defaultdict(dict)
    
    def _init_logger(self, loglevel):
        """
        Init logger data structure
        """
        self.setLevel(loglevel)
        formatter = logging.Formatter('%(levelname)s %(asctime)s %(message)s')
        file_handler = logging.FileHandler(self.logname + '.log')
        file_handler.setFormatter(formatter)
        std_handler = logging.StreamHandler(sys.stdout)
        std_handler.setFormatter(formatter)

        self.addHandler(file_handler)
        self.addHandler(std_handler)
        # return logger
    
    def _set_meta(self, attr_name, logname=None, db=db, loglevel=logging.INFO):
        self.attr_name = attr_name
        self.logname = logname if logname else self.attr_name
        self.db = db
        self._init_logger(loglevel=loglevel)
    
    def _unset_meta(self):
        self.attr_name = ''
        self.handlers = []

    def _get_stackinfo(self, level=2):
        """level: relative stack pos to current stack"""
        st = inspect.stack()[level]
        return st.filename, st.function, st.lineno
    
    def info(self, s, level=2, **kwargs):
        filename, func, lineno = self._get_stackinfo(level=level)
        super().info(f'[{filename} {func}:{lineno}] \n {s}', **kwargs)
    
    def warn(self, s, level=2, **kwargs):
        filename, func, lineno = self._get_stackinfo(level=level)
        super().warn(f'[{filename} {func}:{lineno}] \n {s}', **kwargs)
    
    def debug(self, s, level=2, **kwargs):
        filename, func, lineno = self._get_stackinfo(level=level)
        super().debug(f'[{filename} {func}:{lineno}] \n {s}', **kwargs)
    
    def error(self, s, level=2, **kwargs):
        filename, func, lineno = self._get_stackinfo(level=level)
        super().error(f'[{filename} {func}:{lineno}] \n {s}', **kwargs)
    
    def critical(self, s, level=2, **kwargs):
        filename, func, lineno = self._get_stackinfo(level=level)
        super().critical(f'[{filename} {func}:{lineno}] \n {s}', **kwargs)

    def wayback_url(self, url, wayback):
        self.update_data[url]['wayback_url'] = wayback
        self.info(f'Wayback: {wayback}', level=3)
    
    def title(self, url, title, titlewosuffix=None):
        self.update_data[url]['title'] = title
        if titlewosuffix:
            self.update_data[url]['title'] = titlewosuffix
        self.info(f'title: {title}', level=3)
    
    def topN(self, url, topN):
        self.update_data[url]['topN'] = topN
        self.info(f'topN: {topN}', level=3)

    def token(self, url, available_tokens):
        self.update_data[url]['token'] = available_tokens
        self.info(f'tokens: {available_tokens}', level=3)
    
    def search_results(self, url, engine, typee, results):
        """
        typee: topN/title_site/title_exact
        engine: google/bing
        """
        if f"search_{typee}" not in self.update_data[url]:
            self.update_data[url][f"search_{typee}"] = {'google': [], 'bing':[]}
        self.update_data[url][f"search_{typee}"][engine] = results
        self.info(f'Search results ({typee} {engine}): \n {results}', level=3)
    
    def discover(self, url, backlink, backlink_wayback, status, reason, archive=None, live=None):
        """
        reason: orig_reason (found|notfound|loop)
        """
        self.update_data[url].setdefault('discover', [])
        record = {
            'backlink': backlink,
            'backlink_wayback': backlink_wayback,
            'status': status,
            "reason": reason
        }
        if archive: record.update({'archive': archive})
        # TODO: mis is just the temporary var, need to be formalized
        if live: record.update({'live': live})
        self.update_data[url]['discover'].append(record)
        self.info(f"Backlink: {status} {reason} {archive if archive else ''}", level=3)
    
    def discover_len(self, url):
        return len(self.update_data[url]['discover'])

    def backpath_findpath(self, url, path):
        self.update_data[url].setdefault('backpath', [])
        path_dict = path.to_dict()
        del(path_dict['url'])
        self.update_data[url]['backpath'] = path_dict
        self.info(f"Backpath: {path}", level=3)
    
    def early_exit(self, url):
        self.update_data[url].setdefault('discover', [])
        record = {
            'status': "early exit"
        }
        self.update_data[url]['discover'].append(record)
        self.info(f'discoverer has met too many no snapshot pages, early exit.', level=3)

    def inference(self, url, meta, inputs, reorg):
        self.update_data[url].setdefault('inference', [])
        self.update_data[url]['inference'].append({
            'meta': meta,
            'inputs': inputs,
            'reorg_url': reorg
        })
        self.info(f'Inference: {url} --> {reorg}')
    
    def flush(self):
        self.info(f'Flushing URL(s)')
        for url, d in self.update_data.items():
            try:
                db_d = self.db.traces.find_one({'url': url})
                db_d = db_d.get(self.attr_name, {}) if db_d else {}
                db_d.update(d)
                self.db.traces.update_one({'url': url}, {'$set': {self.attr_name: db_d}}, upsert=True)
            except Exception as e:
                self.warn(f'flush exception {url}: {str(e)}')
        self.update_data = defaultdict(dict)
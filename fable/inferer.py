from xmlrpc.client import ServerProxy
import pandas as pd
import numpy as np
import pickle
from urllib.parse import urlsplit, parse_qsl, parse_qs, unquote
from collections import defaultdict, OrderedDict, Counter
import string
import time
import socket
import os
import regex
import random

from . import config, tools, tracer, verifier
from .utils import crawl, sic_transit, url_utils

import logging
logging.setLoggerClass(tracer.tracer)
tracer = logging.getLogger('logger')
logging.setLoggerClass(logging.Logger)

ISNUM = lambda x: type(x).__module__ == np.__name__ or isinstance(x, int)
VERTICAL_BAR_SET = '\u007C\u00A6\u2016\uFF5C\u2225\u01C0\u01C1\u2223\u2502\u0964\u0965'


def normal_hostname(hostname):
    hostname = hostname.split(':')[0]
    # hostname = hostname.split('.')
    # if hostname[0] == 'www': hostname = hostname[1:]
    # return '.'.join(hostname)
    return hostname

def soft_404_content(reason):
    if not isinstance(reason, list):
        return False
    for r in reason:
        if r != "Similar soft 404 content":
            return False
    return True

class Inferer:
    def __init__(self, proxies={}, memo=None, similar=None):
        self.PS = crawl.ProxySelector(proxies)
        self.proxy = ServerProxy(config.RPC_ADDRESS, allow_none=True)
        self.memo = memo if memo is not None else tools.Memoizer()
        self.similar = similar if similar is not None else tools.Similar()
        self.not_workings = set() # Seen broken inferred URLs
        self.site = None
        self.url_aliases = defaultdict(set) # * Reorg pairs that have been added
        self.url_meta = {} # * {URL: Meta}
        self.upd = url_utils.URLPatternDict(max_diff=2) # * URLPatternDict for clustering input URLs

    def init_site(self, site):
        if self.site:
            self.clear_site()
        self.site = site

    def clear_site(self):
        self.site = None
        self.url_aliases = defaultdict(set)
        self.url_meta = {}
        self.upd = url_utils.URLPatternDict(max_diff=2)

    def add_url_alias(self, url, meta, reorg):
        """
        Only applies to same domain currently
        meta: [title]
        Return bool on whether success
        """
        # if he.extract(reorg) != he.extract(url):
        #     return False

        if url in self.url_aliases and reorg in self.url_aliases[url]:
            return False
        else:
            self.upd.add_url(url)
            self.url_aliases[url].add(reorg)
        
        if meta[0] == 'N/A':
            meta = list(meta)
            meta[0] = ''
            meta = tuple(meta)
        self.url_meta[url] = meta
        return True
    
    def add_url(self, url, meta):
        """
        Add URLs required to infer
        """
        if url in self.url_meta:
            return
        if meta[0] == 'N/A':
            meta = list(meta)
            meta[0] = ''
            meta = tuple(meta)
        self.upd.add_url(url)
        self.url_meta[url] = meta
    
    def add_urls(self, url_metas):
        for url, meta in url_metas:
            self.add_url(url, meta)
    
    def cluster_examples(self, examples):
        """
        Classify examples base on the same delta. To prevent fail to infer
        If with the same delta: there are multiple aliases with the same netloc_dir, prioritize it

        Return [list of examples in the same delta (even netloc_dir)]
        """
        dedup_set = set()
        new_examples = []
        # * Dedup examples
        for example in examples:
            url, alias = url_utils.url_norm(example[0], ignore_scheme=True), url_utils.url_norm(example[2], ignore_scheme=True)
            if (url, alias) in dedup_set:
                continue
            new_examples.append(example)
            dedup_set.add((url, alias))
        delta_examples = defaultdict(list)
        # for example in new_examples:
        #     url, alias = example[0], example[2]
        #     diff = url_utils.url_alias_diff(url, alias)
        #     delta_examples[diff].append(example)
        # delta_examples = [v for v in delta_examples.values()]
        # max_example_len = max([len(v) for v in delta_examples])
        # if max_example_len > 1:
        #     delta_examples = [d for d in delta_examples if len(d) > 1]
        # delta_examples.sort(reverse=True, key=lambda x: len(x))
        # all_examples = []
        # # * Add examples with same delta + alias with the same netloc_dir in the front
        # for delta_example in delta_examples:
        #     nd_alias = defaultdict(list)
        #     for de in delta_example:
        #         nd = url_utils.netloc_dir(de[2])
        #         nd_alias[nd].append(de)
        #     nd_alias = [d for d in nd_alias.values() if len(d) > 1]
        #     nd_alias.sort(reverse=True, key=lambda x: len(x))
        #     all_examples += nd_alias
        # all_examples += delta_examples
        vr = verifier.Verifier(common_prefix=True)
        for example in new_examples:
            url, title, alias = example[0], example[1][0], example[2]
            vr.add_urlalias(url, alias, title, {'type':'dummy', 'method': 'dummy'})
        clusters = vr._gen_cluster()
        all_examples = []
        for cluster in clusters:
            urlalias = cluster['values']
            examples = []
            for v in urlalias:
                title = vr.url_title[v[0]]
                url, alias = vr._normurl_map.get(v[0], v[0]), vr._normurl_map.get(v[1], v[1])
                examples.append((url, (title,), alias))
            all_examples.append(examples)
        return all_examples

    def infer(self, examples, urls):
        """
        Infer reorg urls of urls by learning the transformation rule in urls
        examples: list of (urls, (other metadata), reorg_url)
        urls: list of (urls, other metadata)
        Two metadata should be in the same format
        split: If the urls are too big, split into multiple shards

        Returns: {url: [possible reorg_url]}
        # TODO: Create more sheets with similar/same #words
        """ 
        def normal(s):
            tokens = regex.split(f'_| [{VERTICAL_BAR_SET}] |[{VERTICAL_BAR_SET}]| \p{{Pd}} ', s)
            if len(tokens) > 1:
                s = tokens[0]
            li = string.digits + string.ascii_letters + ' _-'
            rs = ''
            for ch in s:
                if ch in li: rs += ch
                elif ch == "'": continue
                else: rs += ' '
            return rs
        
        def insert_url(sheet, row, url):
            """Insert the original (broken) URL part into the sheet"""
            url = unquote(url)
            us = urlsplit(url)
            path_list = list(filter(lambda x: x != '', us.path.split('/')))
            url_inputs = [normal_hostname(us.netloc)] + path_list
            for j, url_piece in enumerate(url_inputs):
                sheet.loc[row, f'URL{j}'] = url_piece
            qs = url_utils.my_parse_qs(us.query)
            for key, value in qs.items():
                if key == 'NoKey':
                    sheet.loc[row, f'Query_{key}'] = value[0].lower()
                else:
                    sheet.loc[row, f'Query_{key}'] = f'{key}={value[0].lower()}'
            return sheet

        def insert_metadata(sheet, row, meta, expand=True):
            """Expand: Whether to expand the metadata into different form"""
            if ' '.join(meta) == '':
                return sheet
            for j, meta_piece in enumerate(meta):
                if expand:
                    sheet.loc[row, f'Meta{j}'] = normal(meta_piece)
                    sheet.loc[row, f'Meta{j+0.5}'] = normal(meta_piece.lower())
                else:
                    sheet.loc[row, f'Meta{j}'] = meta_piece
            return sheet
        
        def insert_reorg(sheet, row, reorg):
            """Insert alias part into the sheet"""
            reorg = unquote(reorg)
            us_reorg = urlsplit(reorg)
            path_reorg_list = list(filter(lambda x: x != '', us_reorg.path.split('/')))
            url_reorg_inputs = [f"http://{normal_hostname(us_reorg.netloc)}"] + path_reorg_list
            for j, reorg_url_piece in enumerate(url_reorg_inputs):
                sheet.loc[row, f'Output_{j}'] = reorg_url_piece
                qs_reorg = url_utils.my_parse_qs(us_reorg.query)
            for key, value in qs_reorg.items():
                if key == 'NoKey':
                    sheet.loc[i, f'Output_Q_{key}'] = value[0].lower()
                else:
                    sheet.loc[i, f'Output_Q_{key}'] = f'{key}={value[0].lower()}'
            return sheet
                
        sheet1 = pd.DataFrame() # Both url and meta
        sheet2 = pd.DataFrame() # Only meta
        sheet3 = pd.DataFrame() # Only URL
        # * Input examples
        for i, (url, meta, reorg_url) in enumerate(examples):
            # * Input URL part
            sheet1 = insert_url(sheet1, i, url)
            sheet3 = insert_url(sheet3, i, url)
            # * Input Metadata part
            sheet1 = insert_metadata(sheet1, i, meta, expand=True)
            sheet2 = insert_metadata(sheet2, i, meta, expand=True)
            # * Input Reorg Part
            sheet1 = insert_reorg(sheet1, i, reorg_url)
            sheet2 = insert_reorg(sheet2, i, reorg_url)
            sheet3 = insert_reorg(sheet3, i, reorg_url)

        url_idx = {}
        # * Input toinfer examples
        for i, (url, meta) in enumerate(urls):
            counter = i+len(examples)
            url_idx[url] = counter
            # * Input URL part
            sheet1 = insert_url(sheet1, counter, url)
            sheet3 = insert_url(sheet3, counter, url)
            # * Input Metadata part
            sheet1 = insert_metadata(sheet1, counter, meta, expand=True)
            sheet2 = insert_metadata(sheet2, counter, meta, expand=True)
        
        # * RPC formatted dataframe to FlashFill
        sheets = [sheet1, sheet2, sheet3]
        sheets = [s for s in sheets if s.isnull().values.any()]
        # print("SHEET LENGTH", (len(sheets)))
        sheets = [pickle.dumps({
            'sheet_name': f'Sheet{i+1}',
            'csv': sheet
        }) for i, sheet in enumerate(sheets)]
        count = 0
        while count < 3:
            try:
                # socket.setdefaulttimeout(20)
                outputs = self.proxy.handle(sheets, self.site + str(time.time()))
                # socket.setdefaulttimeout(None)
                break
            except Exception as e:
                tracer.error(f'infer: exception on RPC {str(e)}')
                count += 1
                time.sleep(2)
                continue
        if count == 3:
            return {}
        outputs = pickle.loads(outputs.data)
        outputs = [o['csv'] for o in outputs]
        poss_infer = defaultdict(list) # * Any results inferred from 3 sheets
        seen_reorg = set()
        for output in outputs:
            reorg_url_lists = output.filter(regex='^Output_\d', axis=1)
            reorg_query_lists = output.filter(regex='^Output_Q', axis=1)
            for url, meta in urls:
                idx = url_idx[url]
                if idx >= output.shape[0]: # ? Weird bug here: idx exceeds sheet
                    continue
                reorg_url_list = reorg_url_lists.iloc[idx]
                reorg_query_list = reorg_query_lists.iloc[idx]
                num_url_outputs = len(reorg_url_list)
                scheme_netloc_col = reorg_url_lists[f'Output_0'].dropna().tolist()
                host_counter = Counter(scheme_netloc_col)
                scheme_netloc = max(host_counter.items(), key=lambda x: x[1])[0]
                reorg_paths = []
                able_infer = True
                for j in range(1, num_url_outputs):
                    reorg_part = reorg_url_list[f'Output_{j}']
                    # TODO: How to deal with nan requires more thoughts
                    if reorg_part != reorg_part: # * Check for NaN value (trick)
                        if j == num_url_outputs - 1: # * Exempt for filename
                            # reorg_part = "random_str:" + ''.join([random.choice(string.ascii_lowercase + string.digits) for _ in range(20)])
                            able_infer = False
                        # * Instead of continue, pick the most common string if there are multiple same str
                        else:
                            reorg_url_col = reorg_url_lists[f'Output_{j}'].dropna().tolist()
                            path_counter = Counter(reorg_url_col)
                            reorg_part = max(path_counter.items(), key=lambda x: x[1])[0]
                        # continue

                    if ISNUM(reorg_part): reorg_part = str(int(reorg_part))
                    reorg_paths.append(reorg_part)
                if not able_infer:
                    continue
                reorg_paths = '/'.join(reorg_paths)
                reorg_url = f'{scheme_netloc}/{reorg_paths}'
                reorg_queries = []
                for key, reorg_kv in reorg_query_list.items():
                    if reorg_kv != reorg_kv:
                        continue
                    if key != "Output_Q_NoKey" and (len(reorg_kv.split('='))<2 or not reorg_kv.split('=')[1]):
                        continue
                    if ISNUM(reorg_kv): reorg_kv = str(int(reorg_kv))
                    reorg_queries.append(reorg_kv)
                if len(reorg_queries) > 0:
                    reorg_url += f"?{'&'.join(reorg_queries)}"
                
                if reorg_url not in seen_reorg:
                    tracer.inference(url, meta, examples, reorg_url)
                    seen_reorg.add(reorg_url)
                poss_infer[url].append(reorg_url)
        return {k: self._order_alias(v, [examples[0][2]]) for k, v in poss_infer.items()}
    
    def infer_shards(self, examples, urls, split=1000):
        """Wrapper for infer. Only infer in shards to avoid Excel OLE"""
        poss_infer = {}
        for st in range(0, len(urls), split):
            surls = urls[st: min(st+split, len(urls))]
            part_poss_infer = self.infer(examples, surls)
            poss_infer.update(part_poss_infer)
        return poss_infer

    def _construct_input_output(self, match):
        """
        Given a pattern of URLs, output a sheet for RPC inference

        Return: examples: [(url, meta, reorg)], to_infer: [(url, meta)]
        """
        tracer.debug(f"_construct_input_output: {match['pattern']}")
        matched_urls = match['urls']
        output_upd = url_utils.URLPatternDict(max_diff=2)
        examples, toinfer = [], []
        alias_url = defaultdict(list) # * Reverse index
        # * Filter out multiple inputs having same output URL
        for matched_url in matched_urls: 
            if len(self.url_aliases.get(matched_url, set())) > 0: # * Has alias
                for matched_alias in self.url_aliases[matched_url]:
                    alias_url[matched_alias].append(matched_url)
        for matched_url in matched_urls:
            if len(self.url_aliases.get(matched_url, set())) > 0: # * Has alias
                for matched_alias in self.url_aliases[matched_url]:
                    if len(alias_url[matched_alias]) > 1:
                        continue
                    output_upd.add_url(matched_alias)
            else: # * To infer
                cell = (matched_url, self.url_meta[matched_url])
                toinfer.append(cell)
        # * Construct examples (intput)
        good_outputs = output_upd.pop_matches()
        good_outputs.sort(key=lambda x: len(x['urls']), reverse=True)
        if len(good_outputs) == 0:
            return [], []
        top_good = good_outputs[0]
        # * Pick most common output pattern, and construct sheet
        for good_output in top_good['urls']:
            input_url = alias_url[good_output][0]
            cell = (input_url, self.url_meta[input_url], good_output)
            tracer.inference(input_url, self.url_meta[input_url], examples, good_output)
            examples.append(cell)
        return examples, toinfer
    
    def _filter_multicast(self, examples, possible_infer):
        """Filter out all inferred alias that appeared in mutliple original URL"""
        url_norm = lambda x: url_utils.url_norm(x, ignore_scheme=True, case=True)
        alias_match = defaultdict(set)
        for eurl, _, ealias in examples:
            alias_match[url_norm(ealias)].add(url_norm(eurl))
        new_possible_infer = defaultdict(list)
        for infer_url, cands in possible_infer.items():
            for cand in cands:
                cand_html, cand = self.memo.crawl(cand, final_url=True)
                if not cand_html:
                    continue
                cand = crawl.get_canonical(cand, cand_html)
                alias_match[url_norm(cand)].add(url_norm(infer_url))
                new_possible_infer[infer_url].append(url_norm(cand))
        # import json
        # print(json.dumps({k: list(v) for k, v in alias_match.items()}, indent=2))
        return {k: [vv for vv in v if len(alias_match[vv]) <= 1] for k, v in new_possible_infer.items()}

    def infer_on_example(self, example):
        """
        When given a new example, infer all to-find related
        Return: {url: (found_alias, reason)}
        """
        url, meta, alias = example
        self.url_aliases[url].add(alias)
        self.url_meta[url] = meta
        self.upd.add_url(url)
        matched_urls = self.upd.match_url(url)
        found_alias = {}
        for match in matched_urls:
            examples, toinfer = self._construct_input_output(match)
            tracer.info(f"constructed sheet {match['pattern']} (len(examples)/len(toinfer)): {len(examples)}/{len(toinfer)}")
            if len(examples) == 0:
                tracer.debug(f'infer_all: No (enough) inputs can be constructed from this pattern')
                continue
            possible_infer = self.infer(examples, toinfer)
            possible_infer = self._filter_multicast(possible_infer)
            for infer_url, cands in possible_infer.items():
                alias, reason = self._verify_alias(infer_url, cands)
                if alias:
                    tracer.info(f"Found by infer: {infer_url} --> {alias} reason: {reason['type']}")
                    found_alias[infer_url] = (alias, reason)
        return found_alias
    
    def infer_url(self, urlmeta):
        """
        Note: toinfer URL should not be added as alias
        urlmeta: (url, meta) to infer
        Return: {url: (found_alias, reason)} with only target toinfer in the dict
        """
        url, meta = urlmeta
        self.add_url(url, meta)
        matched_urls = self.upd.match_url(url)
        found_alias = {}
        for match in matched_urls:
            examples, toinfer = self._construct_input_output(match)
            tracer.info(f"constructed sheet {match['pattern']} (len(examples)/len(toinfer)): {len(examples)}/{len(toinfer)}")
            if len(examples) == 0:
                tracer.debug(f'infer_all: No (enough) inputs can be constructed from this pattern')
                continue
            possible_infer = self.infer(examples, toinfer)
            possible_infer = self._filter_multicast(possible_infer)
            if len(possible_infer.get(url, [])) <= 0:
                continue
            alias, reason = self._verify_alias(url, possible_infer[url])
            if alias:
                tracer.info(f"Found by infer: {url} --> {alias} reason: {reason['type']}")
                found_alias[url] = (alias, reason)
                return found_alias
        return found_alias

    def infer_all(self):
        """
        Infer on all patterns added to inferer
        
        Return: {url: (found_alias, reason)}
        """
        found_alias = {}
        all_matched = self.upd.pop_matches(least_match=3)
        tracer.info(f'infer_all: number of patterns: {len(all_matched)}')
        for match in all_matched:
            examples, toinfer = self._construct_input_output(match)
            tracer.info(f"constructed sheet {match['pattern']} (len(examples)/len(toinfer)): {len(examples)}/{len(toinfer)}")
            if len(examples) == 0:
                tracer.debug(f'infer_new: No (enough) inputs can be constructed from this pattern')
                continue
            possible_infer = self.infer(examples, toinfer)
            possible_infer = self._filter_multicast(possible_infer)
            for infer_url, cands in possible_infer.items():
                alias, reason = self._verify_alias(infer_url, cands)
                if alias:
                    tracer.info(f"Found by infer: {infer_url} --> {alias} reason: {reason['type']}")
                    found_alias[infer_url] = (alias, reason)
        return found_alias
       
    def _order_alias(self, reorg_urls, example_aliases):
        """Order reorg_urls so that most similar aliases will be tested first"""
        # TODO: example_alias to aliases
        d = OrderedDict()
        for reorg_url in reorg_urls: d[reorg_url] = ''
        reorg_urls = list(d.keys())
        example_alias = example_aliases[0]
        def get_ext(url):
            path = urlsplit(url).path
            if path not in ['/', ''] and path[-1] == '/': path = path[:-1]
            return os.path.splitext(path)[1]
        def query_score(url):
            scores = []
            target_query = urlsplit(example_alias).query
            query = urlsplit(url).query
            # * Has query?
            scores.append(-((target_query != "") == (query != "")))
            target_qs = parse_qs(target_query)
            qs = parse_qs(query)
            # * How many same keys?
            same_keys = set(target_qs.keys()).intersection(qs.keys())
            scores.append(-len(same_keys))
            # * How many same values
            same_values = 0
            for k in same_keys:
                same_values += len(set(target_qs[k]).intersection(qs[k]))
            scores.append(-same_values)
            return tuple(scores)
        def _detect_file_alnum(url):
            """Detect whether string has alpha and/or numeric char"""
            path = urlsplit(url).path
            if path not in ['/', ''] and path[-1] == '/': path = path[:-1]
            filename = os.path.basename(path)
            typee = ''
            alpha_char = [c for c in filename if c.isalpha()]
            num_char = [c for c in filename if c.isdigit()]
            if len(alpha_char) > 0:
                typee += 'A'
            if len(num_char) > 0:
                typee += 'N'
            return set(typee)
        lambdas = []
        # * Same ext?
        lambdas.append(lambda x: -(get_ext(example_alias) == get_ext(x)) )
        # * Has query? Same Key? Same Value?
        lambdas.append(lambda x: query_score(x))
        # * Format similarity
        lambdas.append(lambda x: -len(_detect_file_alnum(example_alias).intersection(_detect_file_alnum(x))))
        reorg_score = []
        for reorg in reorg_urls:
            score = tuple(l(reorg) for l in lambdas)
            reorg_score.append((reorg, score))
        reorg_score.sort(key=lambda x: x[1])
        return [r[0] for r in reorg_score]

    def _verify_alias(self, url, reorg_urls, compare=True):
        """
        reorg_urls: all urls infered by inferer
        compare: whether to actually compare the content/title
        return: Matched URLS, trace(dict)
        """
        reorg_content = {}
        reorg_title = {}
        working_aliases = []
        # * 1. Check breakage of inferred candidates
        new_reorg = False
        for reorg_url in reorg_urls:
            if urlsplit(url).path not in ['', '/'] and urlsplit(reorg_url).path in ['', '/']:
                continue
            new_reorg = True
            if reorg_url in self.not_workings:
                tracer.debug(f'Inferred URL already checked broken: {reorg_url}')
                continue
            # reorg_broken, reason = sic_transit.broken(reorg_url, html=True)
            # if reorg_broken == True and not soft_404_content(reason): # * Broken
            #     self.not_workings.add(reorg_url)
            # else:
            #     reorg_url_html, reorg_url = self.memo.crawl(reorg_url, final_url=True)
            #     reorg_url = crawl.get_canonical(reorg_url, reorg_url_html)
            #     working_aliases.append(reorg_url)
            # ? Try new version
            try:
                if sic_transit.broken(reorg_url, html=True, redir_home=True)[0] != False:
                    raise
                reorg_url_html, reorg_url = self.memo.crawl(reorg_url, final_url=True)
                reorg_url = crawl.get_canonical(reorg_url, reorg_url_html)
                working_aliases.append(reorg_url)
            except: pass
            # ? End of Try

        def return_noncompare():
            """No more information available than whether URLs are working or not"""
            nonlocal working_aliases, new_reorg
            if len(working_aliases) > 0:
                # ? If len(working_aliases) > 1, pick the closest one
                return working_aliases[0], {'type': "nocomp_check", "value": 'N/A'}
            elif not new_reorg:
                return None, {'reason': 'No new reorg actually inferred'}
            else:
                return None, {'reason': 'Inferred urls broken'}

        if not compare:
            return return_noncompare()
        # * Compare version
        # * 2. Get URL's title & Content
        for reorg_url in reorg_urls: # * Check whether reorg_url is broken
            if reorg_url in self.not_workings:
                tracer.debug('Inferred URL already checked broken')
                continue
            reorg_html = self.memo.crawl(reorg_url)
            if reorg_html is None:
                continue
            reorg_content[reorg_url] = self.memo.extract_content(reorg_html)
            reorg_title[reorg_url] = self.memo.extract_title(reorg_html)
        if len(reorg_content) + len(reorg_title) == 0: # * No available content or title
            return return_noncompare()
        wayback_available = False
        try:
            wayback_url = self.memo.wayback_index(url)
            html = self.memo.crawl(wayback_url)
            if html is None: return None, {"reason": "url fail to load on wayback"}
            content = self.memo.extract_content(html)
            title = self.memo.extract_title(html)
            wayback_available = True
        except:
            pass
        # * 3.1 Match title/content
        if wayback_available:
            similar, fromm = self.similar.similar(wayback_url, title, content, reorg_title, reorg_content)[0]
            if similar:
                top_similar = similar
                return top_similar[0], {'type': fromm, 'value': top_similar[1]}
            else:
                return return_noncompare()
        # * 3.2 Match tokens
        else: # * Compare token instead
            alias_tokens = {}
            available_tokens = tools.get_unique_token(url)
            for alias in working_aliases:
                alias_tokens[alias] = url_utils.tokenize_url(alias, process=True)
            token_simi = self.similar.token_similar(url, available_tokens, alias_tokens)[:2]
            if self.similar._separable(token_simi):
                top_similar = token_simi[0]
                return top_similar[0], {'type': "token", 'value': top_similar[-1], 'matched_token': top_similar[1]}
            else:
                return return_noncompare()
    
    def archived_redirected_neighbor(self, url):
        """
        Get archived redirected similar URLs compared to URL
        Used to perform wayback_alias for aliases
        Goes up level by level until see some redirections available

        Return: [matches]
        """
        cur_url = url 
        i = 1
        while i <= 2:
            netdir = url_utils.netloc_dir(cur_url)
            netdir = os.path.join(netdir[0], netdir[1][1:])
            q = os.path.join(netdir, '*')
            param_dict = {
                'url': q,
                "filter": ['mimetype:text/html', 'statuscode:3[0-9]*'],
                'collpase': 'urlkey'
            }
            waybacks, _ = crawl.wayback_index(q, param_dict=param_dict)
            waybacks = [w for w in waybacks if not url_utils.url_match(w[1], url)]
            if len(waybacks) > 0:
                break
            else:
                i += 1
                new_url = url_utils.url_parent(cur_url)
                if urlsplit(new_url).path == urlsplit(cur_url).path:
                    break
                cur_url = new_url
        if len(waybacks) <= 0: 
            return []
        upd = url_utils.URLPatternDict(max_diff=2)
        for _, wayback, _ in waybacks:
            try:
                upd.add_url(wayback)
            except Exception as e:
                continue
        total_urls = set()
        matches = upd.match_url(url, least_match=2, match_ext=True)
        for match in matches: total_urls.update(match['urls'])
        tracer.debug(f'Similar archived redirections: {len(total_urls)}')
        return matches

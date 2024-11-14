import os
from .utils import url_utils, sic_transit, crawl
from collections import defaultdict
import json, regex
import re
from enum import IntEnum

from urllib.parse import urlsplit, urlunsplit, parse_qsl, unquote
VERTICAL_BAR_SET = '\u007C\u00A6\u2016\uFF5C\u2225\u01C0\u01C1\u2223\u2502\u0964\u0965'
OTHER_DELIMITER_SET = '::'

def _throw_unuseful_query(url):
    # return url
    us = urlsplit(url)
    filename, _ = os.path.splitext(us.path.split('/')[-1])
    # if us.query and len(filename) > 5: # ? 5 enough?
    #     us = us._replace(query='')
    if us.query and 'print' in us.query:
        us = us._replace(query='')
    return urlunsplit(us)


class Match(IntEnum):
    PRED = 3
    PREFIX = 2
    MIX = 1
    UNPRED = 0

class MixType(IntEnum):
    ID = 2
    STR = 1
    NA = 0

class URLAlias:
    def __init__(self, url, alias, reason, title=""):
        self.url = url
        self.alias = alias
        self.norm_alias = url_utils.url_norm(alias, ignore_scheme=True, trim_slash=True)
        self.method = reason.get('method', '')
        self.matched = reason.get('type', '')
        self.title = title
        self.others_pairs = []
    
    def to_tuple(self):
        return (self.url, self.alias, self.matched)
    
    def __str__(self):
        return f"{self.to_tuple()}"

    def diffs(self):
        url_tokens = url_utils.tokenize_url(self.url, include_all=True, process='file')
        alias_tokens = url_utils.tokenize_url(self.norm_alias, include_all=True, process='file')
        example_diffs = url_utils.url_token_diffs(url_tokens, alias_tokens)
        return tuple(sorted(e[:2] for e in example_diffs))
    
    def _looks_noid(self, s):
        """s should be int"""
        if not s.isdigit():
            return True
        if int(s) < 100:
            return True
        s = str(s)
        if len(s) >= 4:
            year = int(s[:4])
            if year >= 1900 and year <= 2022:
                return True
        return False

    def get_digit(self, alias=False):
        url = self.alias if alias else self.url
        us = urlsplit(url)
        path, query = us.path, us.query
        digits = []
        tokens = []
        if query:
            qsl = parse_qsl(query)
            for _, v in qsl: tokens.append(v)
        if path != '/' and path[-1] == '/': path = path[:-1]
        path = path.split('/')[1:]
        for i in range(min(2, len(path))):
            tokens.append(path[-(i+1)])
        r_digit = re.compile("((\d+[^a-zA-Z\d\s:]*)+\d+)")
        for t in tokens:
            se = r_digit.findall(t)
            if len(se) <= 0:
                continue
            for s in se:
                s = s[0]
                # * Filter out date
                if self._looks_noid(s):
                    continue
                digits.append(s)
        return digits
    
    def same_digit(self, unique_counter=None):
        """unique_counter: uniqueness check dict for different digits"""
        urld = self.get_digit(alias=False)
        aliasd = self.get_digit(alias=True)
        if unique_counter:
            urld = [d for d in urld if unique_counter.get(d, 1) <= 1]
            aliasd = [d for d in aliasd if unique_counter.get(d, 1) <= 1]
        return len(set(urld).intersection(aliasd)) > 0
    
    def get_token(self, alias=False):
        url = self.alias if alias else self.url
        last_tokens = url_utils.tokenize_url(url, process='file')[-1].split()
        us = urlsplit(url)
        if us.query:
            us = us._replace(query='')
        else:
            us = us._replace(path=os.path.dirname(us.path[:-1]))
        url = urlunsplit(us)
        second_last_tokens = url_utils.tokenize_url(url, process='file')[-1].split()
        return [second_last_tokens, last_tokens]
    
    def overlap_token(self):
        """Return: #same token, #diff tokens"""
        url_tokens = self.get_token(alias=False)
        alias_tokens = self.get_token(alias=True)
        max_overlap = None
        for t1 in url_tokens:
            for t2 in alias_tokens:
                t1, t2 = set(t1), set(t2)
                same, diff = len(t1.intersection(t2)), len(t1-t2)+len(t2-t1)
                max_overlap = max_overlap if max_overlap else (same, diff)
                if same > max_overlap[0]:
                    max_overlap = (same, diff)
        return max_overlap 

    def transformation_rules(self, others_pairs=None, common_prefix=False):
        """
        common_prefix: Whether commonp prefix is considered
        """
        if not others_pairs: others_pairs = self.others_pairs
        others_pairs = [o for o in others_pairs if o.to_tuple()[0] != self.to_tuple()[0] and o.to_tuple()[1] != self.to_tuple()[1]]
        
        url_tokens = url_utils.tokenize_url(self.url, include_all=True, process=False)
        alias_tokens = url_utils.tokenize_url(self.norm_alias, include_all=True, process=False)

        def _intersect_prefix(s, i):
            if s.isdigit():
                return False
            for pa in others_pairs:
                pa_tokens = url_utils.tokenize_url(pa.alias, include_all=True, process=False)
                if len(pa_tokens) > i+1 and pa_tokens[i+1] == s:
                    return True
            return False

        def _predictability(s1, s2):
            """Return matched"""
            # * Check total predictable
            # * Exemption: 01 vs. 1
            def _filter_ext(s):
                ss = os.path.splitext(s)
                if len(ss[1]) < 6:
                    return ss[0]
                else:
                    return s
            if s1.isdigit() and s2.isdigit() and int(s1) == int(s2):
                return Match.PRED, MixType.NA
            s1 = _filter_ext(s1)
            s2 = _filter_ext(s2)
            t1 = url_utils.tokenize(s1, stop_words=None, nonstop_words=['\'', ','])
            t2 = url_utils.tokenize(s2, stop_words=None, nonstop_words=['\'', ','])
            t1s, t2s = set(t1), set(t2)
            if len(t2s) == 0 or len(t1s) == 0:
                return Match.UNPRED, MixType.NA
            # * One of the token is length 1 and the other is not
            if len(t2s)+len(t1s) > 2 and len(t1s)*len(t2s) in [len(t1s),len(t2s)]:
                return Match.UNPRED, MixType.NA
            itst = t1s.intersection(t2s)
            ratio1 = len(itst) / len(t1s)
            # print(s1, s2, ratio1, len(itst) / len(t2s))
            if len(itst) / len(t2s) == 1 and ratio1 >= 0.5:
                return Match.PRED, MixType.NA
            # * Separate tokens into digit and non-digit
            # * If there are some digit predictable, partially pred with ID
            # * Else, str partial pred needs to have majority
            ngram = min(2, len(t1s), len(t2s))
            # for token in itst:
            #     if token.isdigit() and not self._looks_noid(token):
            #         return Match.MIX, MixType.ID
            # t1s_alpha = set([t for t in t1s if t.isalpha()])
            # t2s_alpha = set([t for t in t2s if t.isalpha()])
            t1s_ngram = set([tuple(t1[i:i+ngram]) for i in range(len(t1s)+1-ngram)])
            t2s_ngram = set([tuple(t2[i:i+ngram]) for i in range(len(t2s)+1-ngram)])
            # if len(t2s_alpha) > 0:
            #     itst = t1s_alpha.intersection(t2s_alpha)
            #     ratio2 = len(itst) / len(t2s_alpha)
            #     if ratio2 > 0.6: 
                    # return Match.MIX, MixType.STR
            itst = t1s_ngram.intersection(t2s_ngram)
            ratio2 = len(itst) / len(t2s_ngram)
            # print(s1, s2, len(itst), len(t2s_ngram), ratio2)
            if ratio2 >= 0.5:
                return Match.MIX, MixType.STR
            return Match.UNPRED, MixType.NA

        
        titles = regex.split(f'_| [{VERTICAL_BAR_SET}] |[{VERTICAL_BAR_SET}]| \p{{Pd}} |\p{{Pd}}| (?:{OTHER_DELIMITER_SET}) |(?:{OTHER_DELIMITER_SET})', self.title)
        if len(titles) > 1:
            titles = [' '.join(titles[:-1]), ' '.join(titles[1:]), ' '.join(titles)]
        
        rules = []
        hostname, alias_tokens = alias_tokens[0], alias_tokens[1:]
        for i, at in enumerate(alias_tokens):
            # * Check prefix from other_pairs
            best_match = (Match.UNPRED, "")
            if common_prefix:
                if i != len(alias_tokens)-1 and _intersect_prefix(at, i):
                    best_match = (Match.PREFIX, at)
            src_dict = {Match.PREFIX: at, Match.PRED: 'url/title', Match.MIX: 'url/title', Match.UNPRED: 'N/A'}
            # ! URL
            for ut in url_tokens[1:]:
                match, _ = _predictability(ut, at)
                best_match = max(best_match, (match, src_dict[match]))
            # ! Title
            # src_dict = {Match.PREFIX: at, Match.PRED: 'title', Match.MIX: 'title', Match.UNPRED: 'N/A'}
            if best_match[0] < Match.PREFIX:
                for title in titles:
                    match, _ = _predictability(title, at)
                    best_match = max(best_match, (match, src_dict[match]))
            rules.append(best_match)
        return (hostname, rules)

class Verifier:
    def __init__(self, fuzzy=0, debug=0, common_prefix=False, memo=None, similar=None):
        """fuzzy: Whether candidates are found by fuzzy search"""
        self.url_candidates = defaultdict(lambda: defaultdict(set)) # * {url: {cand: {matched}}}
        self.url_title = {}
        self._url_alias_match = defaultdict(dict)
        self.s_clusters = None
        self._g_clusters = None
        self._r_clusters = None
        self._normurl_map = {}
        self.valid_hints = {
            'archive_canonical':10 , 
            'title':1, 'content':1, 
            'inference': 1.5, 
            'wayback_alias':2, 
            'token':0.5, 
            "anchor": 1, 
            # 'redirection': 2
        }
        self._fuzzy = fuzzy
        self._debug = debug
        self._common_prefix = common_prefix
        self._crawled = set()
        self.memo = memo
        self.similar = similar

    def clear(self):
        self.url_candidates = defaultdict(lambda: defaultdict(set)) # * {url: {cand: {matched}}}
        self.url_title = {}
        self._url_alias_match = defaultdict(dict)
        self._crawled = set()
        self.s_clusters = None
        self._g_clusters = None
        self._r_clusters = None

    def _method_str(self, reason):
        return f"{reason['method']}:{reason.get('type', '')}"

    def _url_norm(self, url):
        normed_url = url_utils.url_norm(url, ignore_scheme=True, trim_www=True, trim_slash=True)
        normed_url = _throw_unuseful_query(normed_url.lower())
        self._normurl_map[normed_url] = url
        return normed_url

    def _alias_match(self, target_url, url_cand):
        """
        If no self.memo or self.similar, don't do any comparison
        Return: {alias: matched}
        """
        alias_match = self._url_alias_match[target_url]
        if self.memo is None or self.similar is None:
            return alias_match
        try:
            wayback_url = self.memo.wayback_index(target_url)
            if wayback_url:
                wayback_html = self.memo.crawl(wayback_url)
                title, content = self.memo.extract_title_content(wayback_html)
            else:
                return alias_match
        except:
            return alias_match
        
        self.similar.separable = lambda x: x[0][1] >= self.similar.threshold
        cands_contents = {}
        cands_titles = {}
        cands = [c[1] for c in url_cand if url_utils.url_match(target_url, c[0])]
        cands_htmls = {}
        for cand in cands:
            if cand in alias_match and 'fuzzy_search' not in alias_match[cand]:
                continue
        
            # # * Sanity check (SE could also got broken pages)
            # if sic_transit.broken(cand, html=True)[0] != False:
            #     continue
            cand_html = self.memo.crawl(cand)
            if cand_html is None: continue
            cands_htmls[cand] = cand_html
            cands_contents[cand] = self.memo.extract_content(cand_html)
            cands_titles[cand] = self.memo.extract_title(cand_html)
        similars = self.similar.similar(wayback_url, title, content, cands_titles, cands_contents,
                                                cands_htmls, shorttext=False)
        if similars[0][0]:
            # * Pre filter suspicous cands
            similars = [(s[0], {"method": "search", "type": fromm, 'value': s[1]}) for s, fromm in similars\
                             if not url_utils.suspicious_alias(target_url, s[0])]
            for a, r in similars:
                self._url_alias_match[target_url][a] = self._method_str(r)
        return self._url_alias_match[target_url]
    

    def add_urlalias(self, url, alias, title, reason):
        url = self._url_norm(url)
        cand = self._url_norm(alias)
        method = self._method_str(reason)
        self.url_candidates[url][cand].add(method)
        self.url_title[url] = title

    def add_aliasexample(self, aliasexmaple, clear=False):
        """
        Transfer raw data of 
            {"alias": [], "examples": []} to url_candidates
        clear: Whether to clear previous url_candidate
        """
        if clear:
            self.clear()
        self._src = 'rw'
        for obj in aliasexmaple['alias']:
            url, cand = obj[0], obj[2]
            if cand is None or len(cand) == 0:
                continue
            # ! Currently go with the last URL. Maybe able to optimize
            if isinstance(cand, list):
                cand = cand[-1]
            url = self._url_norm(url)
            cand = self._url_norm(cand)
            title, reason = obj[1][0], obj[3].copy()
            # * Patch for token match filters
            if reason.get('type') == "token":
                if reason.get('value', 0) < 1:
                    reason['type'] = 'fuzzy_search'
                # else:
                # matched_token = reason['matched_token']
                # if len(matched_token.split(' ')) <= 1:
                #     continue
            if reason['type'] != 'fuzzy_search':
                self._url_alias_match[url][cand] = self._method_str(reason)
            if self._debug:
                if reason['method'] != 'wayback_alias':
                    reason['type'] = 'fuzzy_search'
            method = self._method_str(reason)
            self.url_candidates[url][cand].add(method)
            self.url_title[url] = title
        
        for obj in aliasexmaple['examples']:
            url, cand = obj[0], obj[2]
            if cand is None or len(cand) == 0:
                continue
            # ! Currently go with the last URL. Maybe able to optimize
            if isinstance(cand, list):
                cand = cand[-1]
            url = self._url_norm(url)
            cand = self._url_norm(cand)

            title, reason = obj[1][0], obj[3].copy()
            # * Patch for low value matched tokens
            if reason.get('type') == "token" and reason.get('value', 0) < 0.8:
                continue
            if self._debug:
                if reason['method'] != 'wayback_alias':
                    reason['type'] = 'fuzzy_search'
            method = self._method_str(reason)
            self.url_candidates[url][cand].add(method)
            self.url_title[url] = title
    
    def add_gtobj(self, gt_obj, clear=False):
        """
        Transfer raw data of gt_obj to url_candidates
        clear: Whether to clear previous url_candidate
        """
        if clear:
            self.clear()
        self._src = 'gt'
        url = gt_obj['url']
        url = self._url_norm(url)
        self.url_title[url] = gt_obj.get('title', '')
        # * Search
        search_aliases = gt_obj.get('search', None)
        if search_aliases:
            if not isinstance(search_aliases[0], list): search_aliases = [search_aliases]
            for search_alias in search_aliases:
                if search_alias[0]:
                    search_alias[1]['method'] = 'search'
                    reason = search_alias[1].copy()
                    alias = self._url_norm(search_alias[0])
                    if reason['type'] != 'fuzzy_search':
                        self._url_alias_match[url][alias] = self._method_str(reason)
                    if self._debug:
                        if reason['method'] != 'wayback_alias':
                            reason['type'] = 'fuzzy_search'
                        pass
                    self.url_candidates[url][alias].add(self._method_str(reason))
        # * Backlink
        backlink_alias = gt_obj.get('backlink', None)
        if backlink_alias is not None and backlink_alias[0] is not None:
            backlink_alias[1]['method'] = 'backlink'
            reason = backlink_alias[1].copy()
            alias = self._url_norm(backlink_alias[0])
            if reason['type'] != 'fuzzy_search':
                self._url_alias_match[url][alias] = self._method_str(reason)
            if self._debug:
                if reason['method'] != 'wayback_alias' and reason['type'] != 'archive_canonical':
                    reason['type'] = 'fuzzy_search'
                pass
            self.url_candidates[url][alias].add(self._method_str(reason))
        # * Inference
        infer_alias = gt_obj.get('inference', None)
        if infer_alias is not None and infer_alias[0] is not None:
            infer_alias[1]['method'] = 'inference'
            reason = infer_alias[1].copy()
            alias = self._url_norm(infer_alias[0])
            if reason['type'] != 'fuzzy_search':
                self._url_alias_match[url][alias] = self._method_str(reason)
            if self._debug:
                if reason['method'] not in ['wayback_alias', 'inference']:
                    reason['type'] = 'fuzzy_search'
                pass
            self.url_candidates[url][alias].add(self._method_str(reason))
        
        # * Prepare for examples
        examples = gt_obj.get('examples', [])
        for example in examples:
            ex_url = example[0]
            ex_cand = example[2]
            # TODO Currently only consider the lastest candidate version. May change in the future
            if isinstance(ex_cand, list): ex_cand = ex_cand[-1]
            ex_url = self._url_norm(ex_url)
            ex_cand = self._url_norm(ex_cand)
            ex_title = example[1][0]
            self.url_title[ex_url] = ex_title
            reason = example[3].copy()
            if self._debug:
                if reason['method'] != 'wayback_alias':
                    reason['type'] = 'fuzzy_search'
                if reason['method'] == 'redirection':
                    continue
                pass
            self.url_candidates[ex_url][ex_cand].add(self._method_str(reason))  

    def _filter_suspicious_cands(self):
        new_url_candidates = defaultdict(lambda: defaultdict(set))
        # * Filter cands that looks suspicious
        for url, cands in self.url_candidates.items():
            for cand, v in cands.items():
                if url_utils.suspicious_alias(url, cand):
                    continue
                new_url_candidates[url][cand] = v
        url_candidates = new_url_candidates
        new_url_candidates = defaultdict(lambda: defaultdict(set))
        cand_urls = defaultdict(set)
        # * Filter cands that match to multiple URLs
        for url, cands in url_candidates.items():
            for cand, v in cands.items():
                if len(v) > 1 or 'search:fuzzy_search' not in v:
                    cand_urls[cand].add(url)
        for url, cands in url_candidates.items():
            for cand, v in cands.items():
                if len(cand_urls[cand]) > 2:
                    continue
                new_url_candidates[url][cand] = v
        return new_url_candidates

    def _gen_cluster(self):
        """
        Generate clusters from self.url_candidates
        1. Filter suspicious candidates
        2. Form clusters
        Return cluster: [{pattern, [candidates]}]
        """
        url_candidates = self._filter_suspicious_cands()
        all_pairs = []
        for url, candidates in url_candidates.items(): 
            for cand in candidates:
                all_pairs.append(URLAlias(url, cand, {}))

        cluster = defaultdict(list)
        for turl, tcands in url_candidates.items():
            title = self.url_title.get(turl, '')
            for tcand, reason in tcands.items():
                if not self._fuzzy:
                    if len(reason) > 1 and 'search:fuzzy_search' in reason:
                        reason.remove('search:fuzzy_search')
                ua = URLAlias(turl, tcand, {}, title=title)
                rule = ua.transformation_rules(common_prefix=self._common_prefix, others_pairs=all_pairs)
                rule = (rule[0], tuple([r for r in rule[1]]))
                ua_tuple = list(ua.to_tuple())
                ua_tuple[-1] = '+'.join(reason)
                cluster[rule].append(ua_tuple)
        cluster = [{'values': v, "rule": [k[0],list(k[1])]} for k, v in cluster.items()]
        return cluster
     
    def _rank_cluster(self, cluster):
        def __predictability(rule):
            if self._fuzzy:
                length = len(rule)
                pred = sum([(i+1)/length for i, r in enumerate(rule) if r[0] == 0])
            else:
                pred = len([r for r in rule if r[0] == 0])
            return -pred
        cluster_score = []
        for c in cluster:
            seen_orig_url = set()
            seen_hints = set()
            pred = __predictability(c['rule'][1])
            if pred <= -len(c['rule'][1]):
                    continue 
            for url, cand, method in c['values']:
                seen_orig_url.add(url)
                method = method.split('+')
                method = [m.split(":")[0] for m in method] + [m.split(":")[1] for m in method]
                seen_hints.update(set(self.valid_hints.keys()).intersection(method))
            hint_score = sum([self.valid_hints[s] for s in seen_hints])
            if self._fuzzy:
                # ! Diff1. Ground truth
                if self._src == 'gt' and len(c['values']) == 1 and tuple(c['rule'][1][-1]) < (1, ""):
                    continue
                # ! Diff2. Real-world
                elif self._src == 'rw' and len(c['values']) == 1:
                    continue
                cand_url = defaultdict(set)
                for url, cand, method in c['values']:
                    cand_url[cand].add(url)
                # if max([len(v) for v in cand_url.values()]) > 1:
                #     continue
                cluster_score.append((c, (hint_score, pred, len(seen_orig_url))))         
            else:
                if hint_score > 0:
                    cluster_score.append((c, (hint_score, pred, len(seen_orig_url))))
        return sorted(cluster_score, key=lambda x: x[1], reverse=True)

    def _satisfied_cluster(self, cluster, top_clusters):
        def __more_trustable(r1, r2):
            """Whether r1 is more trustable than r2"""
            if r1[0] != r2[0]:
                return False
            r1, r2 = r1[1], r2[1]
            if len(r1) != len(r2):
                return False
            its = len(r1)
            good = True
            for i in range(-1, -its-1, -1):
                if r1[i][:2] == r2[i][:2]:
                    continue
                if r1[i][:2] < r2[i][:2]:
                    good = False
                    break
            return good
        final_clusters = top_clusters
        for c in cluster[1:]:
            for top_cluster in top_clusters:
                if __more_trustable(c['rule'], top_cluster['rule']):
                    final_clusters.append(c)
                    break
        return final_clusters

    def _valid_cluster(self, cluster, target_url):
        """Check whether the target (top) cluster looks valid"""
        url_cand = defaultdict(set)
        cand_url = defaultdict(set)
        def _norm(url):
            url = _throw_unuseful_query(url)
            return url_utils.url_norm(url, ignore_scheme=True, trim_slash=True)

        valid = False
        for url, cand, method in cluster['values']:
            # * Archive canonical considered as valid automatically
            if url == target_url and ('archive_canonical' in method):
                valid = True
            url = _norm(url)
            cand = url_utils.url_norm(cand, ignore_scheme=True, trim_www=True, trim_slash=True)
            url_cand[url].add(cand)
            cand_url[cand].add(url)

        # * Target URL matched to more than 3 candidates in the cluster
        if not valid and len(url_cand[_norm(target_url)]) >= 4:
            return False

        # * Cluster has no matched property at all
        # total_matched = 0
        # for _, _, method in cluster['values']:
        #     total_matched += self.valid_hints.get(method.split(':')[1], 0)
        # # print(total_matched)
        # if total_matched <= 1:
        #     return False
        return True

    def _more_property_match(self, cluster, target_url):
        """Only used when _fuzzy=1 (all fuzzy_search). Fetch more information for title/content match when the pattern is too general"""
        url_cand = defaultdict(set)
        def _norm(url):
            url = _throw_unuseful_query(url)
            return url_utils.url_norm(url, ignore_scheme=True, trim_slash=True)

        valid = False
        for url, cand, method in cluster['values']:
            # * Archive canonical considered as valid automatically
            if url == target_url and ('archive_canonical' in method):
                valid = True
            url = _norm(url)
            cand = url_utils.url_norm(cand, ignore_scheme=True, trim_www=True, trim_slash=True)
            url_cand[url].add(cand)

        # * Target URL matched to less equal than 3 candidates in the cluster (pattern already selective enough)
        if not valid and len(url_cand[_norm(target_url)]) <= 3:
            return cluster
        
        alias_match = self._alias_match(target_url, cluster['values'])
        new_cluster = {'values': [], 'rule': cluster['rule']}
        new_ua_methods = {'matched': [], 'notmatched': []}
        for v in cluster['values']:
            if v[0] != target_url: 
                new_cluster['values'].append(v)
                continue
            self._crawled.add(v[1])
            if v[1] in alias_match:
                v[2] = alias_match[v[1]]
                new_ua_methods['matched'].append(v)
            else:
                new_ua_methods['notmatched'].append(v)
        if len(new_ua_methods['matched']) > 0:
            new_cluster['values'] += new_ua_methods['matched']
        else:
            new_cluster['values'] += new_ua_methods['notmatched']
        return new_cluster


    def verify_url(self, url):
        """
        verify candidates for url, return [verified obj]
        1. Generate cluster and rank cluster
        2. Get valid clusters
        Return: [(cand, method_str)]
        """
        url = self._url_norm(url)
        if self.s_clusters is None:
            cluster = self._gen_cluster()
            self._g_clusters = cluster
            cluster = self._rank_cluster(cluster)
            self._r_clusters = cluster
            if len(cluster) > 0:
                top_cluster = cluster[0]
                top_clusters = [top_cluster[0]]
                for c, score in cluster[1:]:
                    if len(c['rule'][1]) == len(top_cluster[0]['rule'][1]) and score[1] >= top_cluster[1][1]: top_clusters.append(c)
                    else: break # TODO: Need this policy?
                cluster = [c[0] for c in cluster[len(top_clusters):]]
                self.s_clusters = self._satisfied_cluster(cluster, top_clusters)
            else:
                self.s_clusters = []
        s_clusters = [self._more_property_match(s, url) for s in self.s_clusters]
        s_clusters = [s for s in s_clusters if self._valid_cluster(s, url)]

        # s_clusters = [s for s in self.s_clusters if self._valid_cluster(s, url)]
        if len(s_clusters) == 0:
            s_clusters = [{'values': []}]
        
        valid_cands = []
        for c in s_clusters:
            cand_seen = defaultdict(int)
            for ourl, ocand, method in c['values']:
                cand_seen[ocand] += 1
            for ourl, ocand, method_str in c['values']:
                if ourl != url:
                    continue
                method_str = method_str.split('+')
                method = [m.split(':')[0] for m in method_str]
                matched = [m.split(':')[1] for m in method_str]
                # * 1. cand is found only by fuzzy search 2.1 cand appears for other URLs 2.2 rule for filename is not very predictive
                if matched == ['fuzzy_search']:
                    if cand_seen[ocand] > 1:
                        continue
                    if tuple(c['rule'][1][-1]) < (1, ""):
                        continue
                ocand = self._normurl_map.get(ocand, ocand)
                valid_cands.append((ocand, method_str))
        
        cred = lambda x: sum([sum([self.valid_hints.get(xxx, 0) for xxx in set(xx.split(':'))]) for xx in x[1]])
        valid_cands.sort(reverse=True, key=cred)        
        return valid_cands
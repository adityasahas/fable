from email.policy import default
from fable import histredirector, searcher, inferer, verifier, tools, neighboralias
import os
import logging
from collections import defaultdict
import math

from . import config
from .tracer import tracer as tracing
from .utils import url_utils, crawl, sic_transit

db = config.DB
he = url_utils.HostExtractor()


class AliasFinder:
    def __init__(self, use_db=True, db=db, memo=None, similar=None, proxies={}, tracer=None,\
                classname='fable', logname=None, loglevel=logging.INFO):
        """
        memo: tools.Memoizer class for access cached crawls & API calls. If None, initialize one.
        similar: tools.Similar class for similarity matching. If None, initialize one.
        tracer: self-extended logger
        classname: Class (key) that the db will update data in the corresponding document
        logname: The log file name that will be output msg. If not specified, use classname
        """
        self.memo = memo if memo is not None else tools.Memoizer()
        self.similar = similar if similar is not None else tools.Similar()
        self.PS = crawl.ProxySelector(proxies)
        self.histredirector = histredirector.HistRedirector(memo=self.memo,  proxies=proxies)
        self.searcher = searcher.Searcher(memo=self.memo, similar=self.similar, proxies=proxies)
        self.inferer = inferer.Inferer(memo=self.memo, similar=self.similar, proxies=proxies)
        self.verifier = verifier.Verifier(fuzzy=1, memo=self.memo, similar=self.similar)
        self.nba = neighboralias.NeighborAlias()
        self.db = db
        self.site = None
        self.url_title = {}
        self.classname = classname
        self.logname = classname if logname is None else logname
        self.tracer = tracer if tracer is not None else self._init_tracer(loglevel=loglevel)
        self._candidate_cache = defaultdict(lambda: defaultdict(list)) # * {netloc: {'search/hist_redir': [candidates]}}

    def _init_tracer(self, loglevel):
        logging.setLoggerClass(tracing)
        tracer = logging.getLogger('logger')
        logging.setLoggerClass(logging.Logger)
        tracer._set_meta(self.classname, logname=self.logname, db=self.db, loglevel=loglevel)
        return tracer

    def init_site(self, site, urls=[]):
        self.site = site
        # * Initialized tracer
        if len(self.tracer.handlers) > 2:
            self.tracer.handlers.pop()
        formatter = logging.Formatter('%(levelname)s %(asctime)s %(message)s')
        if not os.path.exists('logs'):
            os.mkdir('logs')
        file_handler = logging.FileHandler(f'./logs/{site}.log')
        file_handler.setFormatter(formatter)
        self.tracer.addHandler(file_handler)

    def clear_site(self):
        self.site = None
        self.pattern_dict = None
        self.seen_reorg_pairs = None
        self.tracer.handlers.pop()
    
    def _get_title(self, url):
        if url in self.url_title:
            return self.url_title[url]
        wayback_url = self.memo.wayback_index(url)
        if wayback_url:
            wayback_html = self.memo.crawl(wayback_url)
            title = self.memo.extract_title(wayback_html)
        else:
            title = ""
        self.url_title[url] = title
        return title

    def infer(self, urls, verified_cands):
        """
        urls: URLs to infer

        Return: [ [url, [title,], [aliases (w/ history)], reason] ]
        """
        first_url = urls[0]
        site = he.extract(first_url)
        if self.similar.site is None or site not in self.similar.site:
            self.similar._init_titles(site)
        self.inferer.init_site(site)

        urlmeta = [[u, [self._get_title(u)]] for u in urls]
        inferexample = []
        for example in verified_cands:
            if not isinstance(example[2], list): example[2] = [example[2]]
            for a in example[2]:
                inferexample.append(example[:2]+[a])
        examples_list = self.inferer.cluster_examples(inferexample)
        alias, reason = None, {}
        urlmeta_dict = {u[0]: u for u in urlmeta}
        url_infer = []
        seen_urls = set()
        for examples in examples_list:
            poss_infer = self.inferer.infer_shards(examples, urlmeta, split=100)
            poss_infer = self.inferer._filter_multicast(examples, poss_infer)
            for url, poss_aliases in poss_infer.items():
                print("POSS ALIAS", poss_aliases)
                alias, reason = self.inferer._verify_alias(url, poss_aliases, compare=False)
                print(alias, reason)
                if alias and url not in seen_urls:
                    seen_urls.add(url)
                    um = urlmeta_dict[url] + [alias, reason]
                    url_infer.append(um)
        nd = url_utils.netloc_dir(urls[0], exclude_index=True)
        self._candidate_cache[nd[0]+nd[1]]['inference'] += url_infer
        return url_infer

    def hist_redir(self, urls):
        """
        Return: [ [url, [title,], [aliases (w/ history)], reason] ]
        """
        hist_aliases = []
        aliases = self.histredirector.wayback_alias_batch_history(urls)

        for url, r in aliases.items():
            title = self._get_title(url)
            reason = {}
            if r:
                reason = {"method": "wayback_alias", "type": "wayback_alias"}
                hist_aliases.append([
                    url,
                    [title],
                    r,
                    reason
                ])
        nd = url_utils.netloc_dir(urls[0], exclude_index=True)
        self._candidate_cache[nd[0]+nd[1]]['hist_redir'] += hist_aliases
        return hist_aliases

    def search(self, urls, nocompare=True, fuzzy=True):
        """
        Search for a set of similar URLs (similar URLs: URLs under the same directory)
        nocompare: True: run search_nocompare, False: search
        fuzzy: if nocompare=False, fuzzy argument for search

        Return: [ [url, [title,], alias, reason] ]
        """
        if isinstance(urls, str): urls = [urls]
        first_url = urls[0]
        site = he.extract(first_url)
        if self.similar.site is None or site not in self.similar.site:
            self.similar._init_titles(site)
        
        search_aliases = []
        for url in urls:
            title = self._get_title(url)
            
            # * Search
            if nocompare:
                aliases = self.searcher.search_nocompare(url, search_engine='bing')
                aliases += self.searcher.search_nocompare(url, search_engine='google')
                aliases = {a[0]: a for a in reversed(aliases) if a[0] is not None}
                aliases = list(aliases.values())
            else:
                aliases = self.searcher.search(url, search_engine='bing', fuzzy=fuzzy)
                if aliases[0] is None:
                    aliases = self.searcher.search(url, search_engine='google', fuzzy=fuzzy)
            
            # * Merge results
            seen = set()
            if len(aliases) > 0 and aliases[0]:
                for a in aliases:
                    reason = a[1]
                    seen.add(a[0])
                    search_aliases.append([url, [title,], a[0], reason])

            all_search = self.searcher.search_results(url)
            for ase in all_search:
                if ase in seen: continue
                seen.add(ase)
                search_aliases.append([url, [title,], ase, {'method': 'search', 'type': 'fuzzy_search'}])
        nd = url_utils.netloc_dir(first_url, exclude_index=True)
        self._candidate_cache[nd[0]+nd[1]]['search'] += search_aliases
        return search_aliases
    
    def verify(self, urls, candidates, neighbor_candididates=[]):
        """
        Verify the candidates found for urls
        candidates & neighbor_candidates: [url, [title,], alias, reason] ]

        Return: verified [ [url, [title,], alias, reason] ]
        """
        # * Form candidates for verifier
        netloc = url_utils.netloc_dir(urls[0], exclude_index=True)
        cand_obj = {'netloc_dir': netloc, 'alias': [], 'examples': []}
        for cand in candidates:
            if cand[0] in urls:
                cand_obj['alias'].append(cand)
        for cand in neighbor_candididates:
            cand_obj['examples'].append(cand)
        
        # * Verify candidates for aliases
        aliases = []
        self.verifier.add_aliasexample(cand_obj, clear=True)
        for url in urls:
            alias = self.verifier.verify_url(url)
            title = self._get_title(url)
            for a, r in alias:
                aliases.append([url, [title,], a, r])
        return aliases
    
    def get_neighbors(self, urls, tss=[], status_filter='23', \
                        max_collect=5):
        """
        Find aliases for urls neighbors using search and/or hist_redir
        urls: str/list. If list, randomly pick 5 (most) and look for their closed neighbors all together
        tss: Timestamps specified to pick closest similar URLs being archived
        max_keep: Max aliases to keep per each URL

        Return: ([broken neighbors], [redirected neighbor URLs with their 'alias'])
        """
        if isinstance(urls, str):
            urls = [urls]
        # * Get neighbor URLs
        ordered_w = self.nba.get_neighbors(urls, tss=tss, status_filter=status_filter)
        # ordered_w = ordered_w[:min(len(ordered_w), max_collect)]
        
        neighbors, aliases = [], []
        tries = 0
        # * Collect URLs for finding aliases
        for _, orig_url, _ in ordered_w:
            if tries > 2 * max_collect:
                break
            broken, reason = sic_transit.broken(orig_url, html=True, redir_home=True)
            title = self._get_title(orig_url)
            if broken != True:
                print(f"URL not broken: {orig_url} {reason}")
                alias = self.nba._non_broken_alias(orig_url)
                tries += 1
                if alias and not url_utils.url_match(orig_url, alias):
                    print(f"redirect alias: {orig_url} --> {alias}")
                    trace = {"method": "redirection", "type": "redirection"}
                    aliases.append((orig_url, (title,), alias, trace))
                continue
            neighbors.append(orig_url)
            if len(neighbors) >= max_collect:
                break
        nd = url_utils.netloc_dir(urls[0], exclude_index=True)
        self._candidate_cache[nd[0]+nd[1]]['redirection'] += aliases
        return neighbors, aliases
    
    def run_all(self, netloc, urls):
        """
        Main workflow func: Combine all techniques
        
        """
        site = he.extract(f"http://{netloc}")
        self.similar._init_titles(site)

        neighbor_urls = []
        neighbor_aliases = []
        if len(urls) < 5:
            neighbor_urls, neighbor_aliases = self.get_neighbors(urls)
        
        cands, neighbor_cands = [], neighbor_aliases
        cands += self.hist_redir(urls)
        cands += self.search(urls)
        if len(neighbor_urls) > 0:
            neighbor_cands += self.hist_redir(neighbor_urls)
            neighbor_cands += self.search(neighbor_urls)

        aliases = self.verify(urls+neighbor_urls, cands, neighbor_cands)
        eurls = set([u[0] for u in aliases])
        iurls = [u for u in urls if u not in eurls]
        if len(iurls) > 0:
            infer_aliases = self.infer(iurls, aliases)
            aliases += infer_aliases
        return aliases
    
    def _early_skip(self):
        """
        Decide whether early escape for the cluster is necessary
        return: Whether need to early_skip
        self.verifier needs to be added with examples
        """
        def _inferrable(rule):
            rule = rule[1]
            for r in reversed(rule[-1:]):
                if r[0] < verifier.Match.MIX: return False
            return True
        for c in self.verifier._g_clusters:
            if not _inferrable(c['rule']):
                continue
            return False
        self.tracer.info(f"_early_skip: Decide to skip")
        return True

    def run_order(self, netloc, urls):
        """
        Main workflow func: Combine all techniques and run in order
        Return: verified [ [url, [title,], alias, reason] ]
        """
        site = he.extract(f"http://{netloc}")
        self.similar._init_titles(site)
        self.histredirector.wayback_index_cache = defaultdict(list)

        cands = []
        # * Get neighbors
        neighbor_urls = []
        neighbor_cands = []
        if len(urls) < 10:
            neighbor_urls, neighbor_cands = self.get_neighbors(urls)
        
        all_urls = set(urls + neighbor_urls)
        url_archived = {}
        for url in all_urls:
            wi = db.wayback_index.find_one({'url': url})
            url_archived[url] = wi and len(wi.get('ts', []))
        url_warchive = [u for u in all_urls if url_archived[u]]
        url_woarchive = [u for u in all_urls if not url_archived[u]]
        all_urls = url_warchive + url_woarchive
        touched_urls = set()
        while len(touched_urls) < len(all_urls):
            untouched_urls = [u for u in all_urls if u not in touched_urls]
            new_url = untouched_urls[0]
            self.tracer.info(f"Test URL: {new_url}")
            new_cands = self.hist_redir([new_url])
            new_cands += self.search([new_url])
            if new_url in urls:
                cands += new_cands
            elif new_url in neighbor_urls:
                neighbor_cands += new_cands
            touched_urls.add(new_url)
            aliases = self.verify(list(touched_urls), cands, neighbor_cands)
            # * Skip check
            N = min(max(2, math.ceil(len(all_urls)*0.4)), len(all_urls))
            if len(aliases) == 0 and len(touched_urls) >= N:
                if self._early_skip():
                    pass
                    # break
            # * Inference
            urls_seen_aliases = set([a[0] for a in aliases])
            toinfer_urls = [u for u in all_urls if u not in urls_seen_aliases]
            if len(toinfer_urls) > 0 and len(urls_seen_aliases) > 1:
                infer_aliases = self.infer(toinfer_urls, aliases)
                touched_urls.update([u[0] for u in infer_aliases])
                aliases += infer_aliases
        url_aliases = defaultdict(list)
        for a in aliases:
            url_aliases[a[0]].append(a)
        aliases = [v[0] for v in url_aliases.values()]
        return aliases
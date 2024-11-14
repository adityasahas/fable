"""
Search broken pages' content
"""
import re, regex, os
from urllib.parse import urlsplit, urlunsplit

from . import  tools, tracer
from .utils import search, crawl, url_utils, sic_transit

import logging
logging.setLoggerClass(tracer.tracer)
tracer = logging.getLogger('logger')
logging.setLoggerClass(logging.Logger)

he = url_utils.HostExtractor()
VERTICAL_BAR_SET = '\u007C\u00A6\u2016\uFF5C\u2225\u01C0\u01C1\u2223\u2502\u0964\u0965'

class Searcher:
    def __init__(self, use_db=True, proxies={}, memo=None, similar=None):
        """
        At lease one of db or corpus should be provided
        # TODO: Corpus could not be necessary

        Return: 
            If found: URL, Trace (how copy is found, etc)
            else: None
        """
        self.PS = crawl.ProxySelector(proxies)
        self.use_db = use_db
        self.memo = memo if memo is not None else tools.Memoizer()
        self.similar = similar if similar is not None else tools.Similar()
        self.searched_results = {} # * URL: {title: {}, content: {}}

    def _check_archive_canonical(self, url, html):
        """
        Check whether archive has the working canonical
        url: wayback form
        html: wayback html

        Return: alias if found, else None
        """
        canonical = crawl.get_canonical(url, html)
        if not url_utils.url_match(url, canonical, wayback=True):
            canonical = url_utils.filter_wayback(canonical)
            if sic_transit.broken(canonical)[0] is False:
                try:
                    live_canonical = crawl.requests_crawl(canonical, raw=True)
                    live_canonical = crawl.get_canonical(live_canonical.url, live_canonical.text)
                    return live_canonical
                except:
                    pass
        return

    def search(self, url, search_engine='bing', fuzzy=False):
        """
        fuzzy: If there will be no separable check for similar

        Return: If no alias: None, reason
                If alias:   fuzzy=False: (top_match, reason)
                            fuzzy=True: [(match, reason)]
        """
        global he

        def _token_search():
            nonlocal fuzzy_similars
            available_tokens = tools.get_unique_token(url)
            tracer.token(url, available_tokens)
            search_results = []
            for i, token in enumerate(available_tokens):
                token = os.path.splitext(token)[0]
                token = regex.split("[^a-zA-Z0-9]", token)
                token = ' '.join(token)
                if search_engine == 'bing':
                    # * Bing
                    search_results = search.bing_search(f'{token} site:{site}', use_db=self.use_db)
                    tracer.search_results(url, 'bing', f"token_{i}", search_results)
                else:
                    # * Google
                    search_results = search.google_search(f'{token}', site_spec_url=site, use_db=self.use_db)
                    tracer.search_results(url, 'google', f"token_{i}", search_results)
                search_tokens = {}
                for sr in search_results:
                    tokens = url_utils.tokenize_url(sr, process=True)
                    search_tokens[sr] = tokens
                token_simi = self.similar.token_similar(url, token, search_tokens, shorttext=shorttext)[:2]
                
                # token_simi = [s for s in token_simi if s[0] != "" and sic_transit.broken(s[0],  html=True)[0] == False]
                token_simi = [s for s in token_simi if not url_utils.suspicious_alias(url, s[0])]
                print("token_simi", token_simi)
                if len(token_simi) == 0:
                    break
                if len(token_simi) == 1:
                    token_simi.append(('', 0, ''))
                if self.similar.separable(token_simi):
                    # top_similar = token_simi[0]
                    # top_similar_url = top_similar[0]
                    # top_similar_html, top_similar_url = self.memo.crawl(top_similar_url, final_url=True)
                    # top_similar_url = crawl.get_canonical(top_similar_url, top_similar_html)
                    rs = [(r[0], {"method": "search", "type": "token", 'value': r[1], 'matched_token': r[-1]}) for r in token_simi if r[0] != ""]
                    if fuzzy:
                        fuzzy_similars += rs
                    else:
                        return rs[0]

        if search_engine not in ['google', 'bing']:
            raise Exception("Search engine could support for google and bing")
        # TODO: Not ideal
        elif search_engine == 'bing':
            self.searched_results = {}
        
        if url_utils.na_url(url):
            return None, {'reason': "Not applicable URL"}
        if url not in self.searched_results:
            self.searched_results[url] ={'title': {}, 'content': {}, 'html': {}}

        if fuzzy:
            self.similar.separable = lambda x: x[0][1] >= self.similar.threshold
            shorttext = False
            fuzzy_similars = []
        else:
            self.similar.separable = None
            shorttext = True

        site = he.extract(url)
        if not site: return None, {'reason': "Fail to get site of URL (non http URL)"}
        if '://' not in site: site = f'http://{site}'
        _, final_url = self.memo.crawl(site, final_url=True)
        if final_url is not None:
            site = he.extract(final_url)
        try:
            wayback_url = self.memo.wayback_index(url)
            html = self.memo.crawl(wayback_url, proxies=self.PS.select())
            title = self.memo.extract_title(html, version='mine')
            content = self.memo.extract_content(html)
            tracer.wayback_url(url, wayback_url)
        except Exception as e:
            tracer.warn(f'Exceptions happen when loading wayback verison of url: {str(e)}') 
            r = _token_search()
            if fuzzy and len(fuzzy_similars) > 0:
                return fuzzy_similars
            elif r is not None:
                return r
            else:
                return None, {'reason': "Fail to get archive copy"}
        
        # * Archive canonical
        canonical_alias = self._check_archive_canonical(wayback_url, html)
        tracer.debug(f'_check_archive_canonical: {canonical_alias}')
        if canonical_alias:
            simi_tup = (canonical_alias, \
                    {'method': 'archive_canonical', 'type': 'archive_canonical', 'value': 'N/A'})
            if fuzzy:
                fuzzy_similars.append(simi_tup)
            else:
                return simi_tup

        tracer.title(url, title)
        search_results, searched = [], set()

        def search_once(search_results, typee):
            """Incremental Search"""
            global he
            nonlocal url, title, content, html, searched, search_engine
            searched_contents = {}
            searched_titles = {}
            search_cand = [s for s in search_results if s not in searched]
            tracer.search_results(url, search_engine, typee, search_results)
            searched.update(search_results)
            searched_htmls = {}
            for searched_url in search_cand:
                # * Sanity check (SE could also got broken pages)
                if sic_transit.broken(searched_url, html=True)[0] != False:
                    tracer.debug(f'search_once: searched URL {searched_url} is broken')
                    continue
                # * Use earliest archived copy if available
                # searched_wayback = self.memo.wayback_index(searched_url, policy='earliest')
                # searched_url_rep = searched_wayback if searched_wayback else searched_url
                searched_url_rep = searched_url
                searched_html, searched_url_rep = self.memo.crawl(searched_url_rep, final_url=True, proxies=self.PS.select())
                if searched_html is None: continue
                searched_url_rep = crawl.get_canonical(searched_url_rep, searched_html)
                searched_htmls[searched_url_rep] = searched_html
                searched_contents[searched_url_rep] = self.memo.extract_content(searched_html)
                if he.extract(url) == he.extract(searched_url) or site == he.extract(searched_url):
                    searched_titles[searched_url_rep] = self.memo.extract_title(searched_html)
            self.searched_results[url]['title'].update(searched_titles)
            self.searched_results[url]['content'].update(searched_contents)
            self.searched_results[url]['html'].update(searched_htmls)
            similars = self.similar.similar(wayback_url, title, content, self.searched_results[url]['title'], self.searched_results[url]['content'],
                                                    self.searched_results[url]['html'], shorttext=shorttext)
            if similars[0][0] is None:
                similars = []
            # * Pre filter suspicous cands
            similars = [(s[0], {"method": "search", "type": fromm, 'value': s[1]}) for s, fromm in similars if not url_utils.suspicious_alias(url, s[0])]
            if fuzzy:
                seen_similar = set([s[0] for s in similars])
                similars += [(s, {"method": "search", "type": 'fuzzy_search', 'value': "N/A"}) for s in search_cand \
                        if not url_utils.suspicious_alias(url, s) and s not in seen_similar]
                return similars
            else:
                return similars[0] if len(similars) > 0 else None

        # * Search with title
        if title != '' and site:
            uniq_title = self.similar.unique_title(wayback_url, title, content, self.similar.wb_meta, wayback=True)
            uniq_title = regex.split(f'_| [{VERTICAL_BAR_SET}] |[{VERTICAL_BAR_SET}]| \p{{Pd}} |\p{{Pd}}', uniq_title)
            if search_engine == 'bing':
                # * Bing Title
                site_str = f'site:{site}'
                bing_title = uniq_title
                bing_title = ' '.join(bing_title)
                bing_title = re.sub(r'[^\x00-\x7F]+', ' ' , bing_title)
                tracer.debug(f'Search query: {bing_title} {site_str}')
                search_results = search.bing_search(f'{bing_title} {site_str}', use_db=self.use_db)
                if len(search_results) > 20: search_results = search_results[:20]
                similar = search_once(search_results, typee='title_site')
                if similar is not None: 
                    if fuzzy:
                        fuzzy_similars += similar
                    else:
                        return similar
                if len(search_results) >= 8:
                    search_results = search.bing_search(f'+"{bing_title}" {site_str}', use_db=self.use_db)
                    if len(search_results) > 20: search_results = search_results[:20]
                    similar = search_once(search_results, typee='title_exact')
                    if similar is not None: 
                        if fuzzy:
                            fuzzy_similars += similar
                        else:
                            return similar
            else:
                google_title = uniq_title
                google_title = ' '.join(google_title)
                google_title = re.sub(r'[^\x00-\x7F]+', ' ' , google_title)
                # * Google Title
                search_results = search.google_search(f'{google_title}', site_spec_url=site, use_db=self.use_db)
                similar = search_once(search_results, typee='title_site')
                if similar is not None: 
                    if fuzzy:
                        fuzzy_similars += similar
                    else:
                        return similar
                if len(search_results) >= 8:
                    search_results = search.google_search(f'"{google_title}"', site_spec_url=site, use_db=self.use_db)
                    similar = search_once(search_results, typee='title_exact')
                    if similar is not None: 
                        if fuzzy:
                            fuzzy_similars += similar
                        else:
                            return similar
        
        # * Search with token
        r = _token_search()
        if not fuzzy and r is not None:
            return r

        # * Search with content
        self.similar.tfidf._clear_workingset()
        topN = self.similar.tfidf.topN(content)
        topN = ' '.join(topN)
        tracer.topN(url, topN)
        search_results = []
        if len(topN) > 0:
            if search_engine == 'bing':
                # * Bing Content
                if site is not None:
                    site_str = f'site:{site}'
                else: 
                    site_str = ''
                search_results = search.bing_search(f'{topN} {site_str}', use_db=self.use_db)
                if len(search_results) > 20: search_results = search_results[:20]
            else:
                # * Google Content
                search_results = search.google_search(topN, site_spec_url=site, use_db=self.use_db)
            similar = search_once(search_results, typee='topN')
            if similar is not None: 
                if fuzzy:
                    fuzzy_similars += similar
                else:
                    return similar
        if fuzzy and len(fuzzy_similars) > 0:
            fuzzy_similars = {f[0]: f for f in reversed(fuzzy_similars)}
            fuzzy_similars = list(fuzzy_similars.values())
            return fuzzy_similars
        else:
            return None, {}
    
    
    def search_nocompare(self, url, search_engine='bing'):
        """
        No comparison version of search. Search results are directly poped out as candidates

        Return: If exception: [(None, reason)]
                Else: [(match, reason)]
        """
        global he

        def _token_search():
            nonlocal fuzzy_similars
            available_tokens = tools.get_unique_token(url)
            tracer.token(url, available_tokens)
            search_results = []
            for i, token in enumerate(available_tokens):
                token = os.path.splitext(token)[0]
                token = regex.split("[^a-zA-Z0-9]", token)
                token = ' '.join(token)
                if search_engine == 'bing':
                    # * Bing
                    search_results = search.bing_search(f'instreamset:url:{token} site:{site}', use_db=self.use_db)
                    tracer.search_results(url, 'bing', f"token_{i}", search_results)
                else:
                    # * Google
                    search_results = search.google_search(f'inurl:{token}', site_spec_url=site, use_db=self.use_db)
                    tracer.search_results(url, 'google', f"token_{i}", search_results)
                search_tokens = {}
                for sr in search_results:
                    tokens = url_utils.tokenize_url(sr, process=True)
                    search_tokens[sr] = tokens
                token_simi = self.similar.token_similar(url, token, search_tokens, shorttext=False)
                front_token_simi = token_simi[:2]

                token_simi = [s for s in token_simi if not url_utils.suspicious_alias(url, s[0])]
                print("token_simi", token_simi)
                if len(token_simi) == 0:
                    break
                if len(token_simi) == 1:
                    token_simi.append(('', 0, ''))
                if self.similar.separable(token_simi):
                    rs = [(r[0], {"method": "search", "type": "token", 'value': r[1], 'matched_token': r[-1]}) for r in token_simi if r[0] != ""]
                    fuzzy_similars += rs
                    
        if search_engine not in ['google', 'bing']:
            raise Exception("Search engine could support for google and bing")
        # TODO: Not ideal
        elif search_engine == 'bing':
            self.searched_results = {}
        
        if url_utils.na_url(url):
            return [(None, {'reason': "Not applicable URL"})]
        if url not in self.searched_results:
            self.searched_results[url] ={'title': {}, 'content': {}, 'html': {}}
        
        self.similar.separable = lambda x: x[0][1] >= self.similar.threshold
        shorttext = False
        fuzzy_similars = []

        site = he.extract(url)
        if not site: 
            return [(None, {'reason': "Fail to get site of URL (non http URL)"})]
        if '://' not in site: site = f'http://{site}'
        _, final_url = self.memo.crawl(site, final_url=True)
        if final_url is not None:
            site = he.extract(final_url)
        try:
            wayback_url = self.memo.wayback_index(url)
            html = self.memo.crawl(wayback_url, proxies=self.PS.select())
            title = self.memo.extract_title(html, version='mine')
            content = self.memo.extract_content(html)
            tracer.wayback_url(url, wayback_url)
        except Exception as e:
            tracer.warn(f'Exceptions happen when loading wayback verison of url: {str(e)}') 
            _token_search()
            if len(fuzzy_similars) > 0:
                return fuzzy_similars
            else:
                return []
        
        # * Archive canonical
        canonical_alias = self._check_archive_canonical(wayback_url, html)
        tracer.debug(f'_check_archive_canonical: {canonical_alias}')
        if canonical_alias:
            fuzzy_similars.append((canonical_alias, \
                {'method': 'archive_canonical', 'type': 'archive_canonical', 'value': 'N/A'}))

        tracer.title(url, title)
        search_results, searched = [], set()

        def search_once(search_results, typee):
            """Incremental Search"""
            global he
            nonlocal url, title, content, html, searched, search_engine
            tracer.search_results(url, search_engine, typee, search_results)
            search_cand = [s for s in search_results if s not in searched]
            searched.update(search_results)    
            # * Pre filter suspicous cands
            similars = [(s, {"method": "search", "type": 'fuzzy_search', 'value': "N/A"}) for s in search_cand \
                            if not url_utils.suspicious_alias(url, s)]
            return similars
        

        # * Search with title_site
        if title != '' and site:
            uniq_title = self.similar.unique_title(wayback_url, title, content, self.similar.wb_meta, wayback=True)
            uniq_title = regex.split(f'_| [{VERTICAL_BAR_SET}] |[{VERTICAL_BAR_SET}]| \p{{Pd}} |\p{{Pd}}', uniq_title)
            # * Title_site
            if search_engine == 'bing':
                # * Bing Title
                site_str = f'site:{site}'
                bing_title = uniq_title
                bing_title = ' '.join(bing_title)
                bing_title = re.sub(r'[^\x00-\x7F]+', ' ' , bing_title)
                tracer.debug(f'Search query: {bing_title} {site_str}')
                search_results = search.bing_search(f'{bing_title} {site_str}', use_db=self.use_db)
            else:
                google_title = uniq_title
                google_title = ' '.join(google_title)
                google_title = re.sub(r'[^\x00-\x7F]+', ' ' , google_title)
                # * Google Title
                search_results = search.google_search(f'{google_title}', site_spec_url=site, use_db=self.use_db)
                tracer.debug(f'Search query: {google_title}')
            similar = search_once(search_results, typee='title_site')
            if similar is not None: 
                fuzzy_similars += similar
   
            # * Title_exact
            if len(search_results) >= 8:
                if search_engine == 'bing':
                    search_results = search.bing_search(f'+"{bing_title}" {site_str}', use_db=self.use_db)
                    if len(search_results) > 20: search_results = search_results[:20]
                else:
                    search_results = search.google_search(f'"{google_title}"', site_spec_url=site, use_db=self.use_db)
                similar = search_once(search_results, typee='title_exact')
                if similar is not None: 
                    fuzzy_similars += similar

                    
        # * Search with token
        _token_search()
  
        # * Search with content
        self.similar.tfidf._clear_workingset()
        topN = self.similar.tfidf.topN(content)
        topN = ' '.join(topN)
        tracer.topN(url, topN)
        search_results = []
        if len(topN) > 0:
            if search_engine == 'bing':
                # * Bing Content
                if site is not None:
                    site_str = f'site:{site}'
                else: 
                    site_str = ''
                search_results = search.bing_search(f'{topN} {site_str}', use_db=self.use_db)
                if len(search_results) > 20: search_results = search_results[:20]
            else:
                # * Google Content
                search_results = search.google_search(topN, site_spec_url=site, use_db=self.use_db)
            similar = search_once(search_results, typee='topN')
            if similar is not None: 
                fuzzy_similars += similar
        fuzzy_similars = {f[0]: f for f in reversed(fuzzy_similars)}
        fuzzy_similars = list(fuzzy_similars.values())
        return fuzzy_similars
        
    
    def search_results(self, url, engines=['bing', 'google']):
        """Get search results from tracer"""
        searches = []
        update_data = tracer.update_data.get(url)
        if update_data is None:
            return []
        for k, v in update_data.items():
            if 'search' in k:
                for engine in engines:
                    searches += v[engine]
        return list(dict.fromkeys(searches).keys())
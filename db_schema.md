# DB: fable/ReorgPageFinder

### crawl
Record all crawled pages and its html
```json
{
    "url": "url (unique indexed)",
    "statuscode": "",
    "html": "byte (hash indexed)",
    "final_url": "string",
    "content": "string (optional)",
    "title": "string (optional)",
    "ttl": "int",
    "site": "Site of the URL"
}
```

### corpus
Used to initialize tfidf for document corpus
```json
{
    "url": "string",
    "src": "string",
    "html": "Bytes (compressed by brotli)",
    "content": "string",
    "Others": "Not useful"
}
```

### wayback_index
Wayback indexed timestamps
```json
{
    "url": "string (unique indexed)",
    "ts": "[int]",
    "ts_nb": "[int] (not broken, including 3xx)"
}
```

### wayback_rep
Wayback most representative ts for a url
```json
{
    "url": "string (unique indexed)",
    "ts": "int",
    "wayback_url": "string",
    "policy": "string",
    "(policy_ts)": "int"
}
```

### searched
##### Index: query_engine (hash) , query_site_engine (hash)
Searched query (siteSearch) with results
```json
{
    "query": "string",
    "engine": "google|bing",
    "(site)": "string",
    "results": ["string"]
}
```

### reorg
Set of URLs for trying to find copies on
```json
{
    "url": "url (hash unique indexed)",
    "hostname": "hostname (Ascending indexed)",
    "title": "title",
    "class (logname)": {
        "by": {
            "method": "Found by what method",
            "type": "matched by what type",
            "value": "Matched type value"
        }
        "reorg_url": "str (Reorganized URL)"
    }
}
```

### na_urls
URLs that are not applicable for finding alternate links
```json
{
    "url": "url (hash unique indexed)",
    "hostname": "hostname",
    "no_snapshot": "Bool (This URL has no snapshots in the wayback)",
    "no_working_parent": "Bool (This url's parent doesn't have snapshot/doesn't link to this url/ doesn't work today)",
    "false_positive": "Bool (If broken detection is false positive)"
}
```

### checked
Set of already tried URLs with certain techniques
```json
{
    "_id": "url",
    "url": "url",
    "hostname": "hostname",

    "search_1": "bool/true (First pass of search)",
    "search_2": "bool/true (Second pass of search)",
    "discover": "bool/true (discover)",
    "infer": "bool/true (used as check for inference test)",
    ("search_coverage"): "bool/true (Test on coverage of search)",
    ("search_gt_10"): "bool/true (Test on hit rate of search with more than 10 results)",
    ("discover_BFS"): "bool/true",
    ("discover_DFS"): "bool/true",
    ("strawman"): "bool/true",
    ("strawman_adv"): "bool/true",
    ("infer_efficiency"): "bool/true",
    ("outlink_matter"): "bool/true"
}
```

### traces
New traces document to trace all metadata gathered during running FABLE
```json
{
    "_id": "url",
    "url": "url",
    "hostname": "hostname",
    "{class} (name of trace)": 
        {
            "wayback_url": "url/None if not applicable",
            "title": "str",
            "title_wo_suffix": "str (title without suffix)",
            "topN": "str",
            "search_topN": {"google": [], "bing": []},
            "search_title_site": {"google": [], "bing": []},
            "search_title_exact": {"google": [], "bing": []},
            "discover": [{
                "backlink",: "url",
                "backlink_wayback": "wayback",
                "status": "str",
                "reason": "str",
                "(link)": "If applicable"
            }],
            "backpath": [{
                "url": "url",
                "sig": "link signature",
                "wayback_url": ["waback_url"]
            }] // Currently only log the found path
        }
}
```

### trace
Traces of detailed execution events (how each urls' alternate links are frying to be found)
```json
{
    "_id": "url",
    "url": "url",
    "hostname": "hostname",
    "search_1": "list of traces for first search",
    "search_2": "list of traces for second search",
    "discover": "list of traces for discovery",
    "backpath_earliest": "Path structure on earliest searched backpath",
    "backpath_latest": "Path structure on latest searched backpath",
    "discover_BFS": "list of traces for discovery",
}
```


### search_trace
Record for performance of search
```json
{
    "_id": "url",
    "url": "url",
    "reorg_url": "reorg_url (if exists)",
    "hostname": "site of url",
    "site": "Actual (final) site of url",
    "title": "str",
    "topN": "str",
    "title_site": {
        "bing": ["list of results"],
        "google": ["list of results"]
    },
    "title_exact": {
        "bing": ["list of results"],
        "google": ["list of results"]
    },
    "topN_site": {
        "bing": ["list of results"],
        "google": ["list of results"]
    }
}
```

### reorg_infer
Record for testing the efficiency of infer
```json
{
    "_id": "url",
    "url": "url",
    "hostname": "hostname",
    "title": "title",
    "reorg_url_search": "reorg_url",
    "by_search": "dict",
    "reorg_url_search": "reorg_url",
    "by_search": "dict",
    "reorg_url_discover": "reorg_url",
    "by_discover": "dict",
    "reorg_url_infer": "",
    "by_infer": ""
}
```

### outlinks
Check matter outlinks
```json
{
    "_id": "url",
    "url": "url",
    "hostname": "hostname",
    "title": "title",
    "most_similar": "url|N/A",
    "similarity": "float"
}
```


## DB: web_decay

### seeds
Union of seeds from DMOZ from 2005, 2011, 2013, 2017. Around 7M links in total
```json
{
    "url": "string",
    "category": "str"
}
```

### host_meta
Hostname encountered on random walking on the wayback machine in certain year
```json
{
    "hostname": "string",
    "year": "int",
}
```

### hosts_added_links
Number of links newly added to wayback machine for host in certain year
```json
{
    "hostname": "string",
    "year": "int",
    "added_links": "int"
}
```

### url_sample
Sampled urls. For every host with >= 500 urls, sample 100 from them
```json
{
    "_id": "url",
    "url": "string",
    "hostname": "string",
    "year": ["int"]
}
```

### host_status
Status that each host has. One host could have different status \
Unique on each obj (all fields composition)
```json
{
    "hostname": "string",
    "year": ["int"],
    "status": "status (2/3/4/5xx / DNS / Other)",
    "detail": "string"
}
```

### url_status
Status for each url, urls with hosts in host_status
Unique on (url + year)
```json
{
    "url": "string",
    "hostname": "string",
    "year": ["int"],
    "status": "status (2/3/4/5xx / DNS / Other)",
    "detail": "string",
    "(similarity)": "TF-IDF similarity of page on liveweb vs. wayback machine"
}
```

### url_year_added
For each hosts in hosts_added_links, all the urls newly crawled on wayback
```json
{
    "url": "string",
    "hostname": "string",
    "year": "int",
}
```

### url_population
only url in url_year_added with host has >=100 new urls in certain year
```json
{
    "url": "string",
    "hostname": "string",
    "year": "int",
}
```

### url_content
HTML and content of url, both from wayback machine and realweb \
src defines the where the html was crawled \
If src: wayback, ts, usage are added. \
usage: represent --> represent the content \
     archive --> Just archive for crawl
     updating --> Used for deterct archiving
```json
{
    "url": "string",
    "src": "string",
    "(ts)":"int",
    "html": "Bytes (compressed by brotli)",
    "content": "string",
    "(usage)": "represent / archive | updating"
}
```

### host_sample
Sample host which contains 2xx/3xx status code
```json
{
    "hostname": "string",
    "year": "int"
}
```

### url_status_implicit_broken
The joined results between url_status and host_sample \
In other word, all urls in url_status which hostname is sampled in host_sample\
```json
{
    "url": "string",
    "hostname": "string",
    "year": ["int"],
    "status": "status (2/3/4/5xx / DNS / Other)",
    "detail": "string",
    "(similarity)": "TF-IDF similarity of page on liveweb vs. wayback machine",
    "(broken)": "Boolean (used as determine hard broken)",
    "(sic_broken)": "Boolean sic_transit's determine 23xx broken",
    "(sic_reason)": "string reason for determine why is (not) broken",
    "ct_broken": "Boolean content broken" 
}
```

### url_update
Record whether a url has high link density / update frequently on wayback
```json
{
    "_id": "string (url)",
    "url": "string",
    "updating": "boolean, false means no / unknown (1 snapshot, similar, etc... )" ,
    "tss": [],
    "detail": "updating True: HLD, not similar, no contents. False: 1 snapshot, no content, similar, no html",
    "(similarity)": "float (only if similar/not similar)",
    "(recordts)": []
}
```

### url_broken
Sampled broken urls from site which contains >= 50 broken urls \
Subset of url_status_implicit_broken
Format: Sampe as url_status_implicit_broken

### url_broken_year
Sampled broken urls. 1000 per cell: {year * status} \
Subset of url_status_implicit_broken
Format: Sampe as url_status 

### search_meta
##### Index: url_ts (unique)
For each broken page, record its html, content and queries for search \
Usage is like url_content
```json
{
    "url": "string",
    "html": "byte (brotli)",
    "ts": "int",
    "content": "string",
    "topN": "string",
    "titleMatch": "string",
    "usage": "(represent | archive)",
    "similarity": "float (-1 means no search result found)",
    "searched_url": "string, urls that has highest simi"
}
```

### search
##### Index: url_from (unique)
Search results for broken pages
```json
{
    "url": "string",
    "from": "string (url)",
    "html": "byte (brotli)",
    "content": "string",
    "rank": "top5 / top10"
}
```

### search_sanity_meta
For sampled good page \
Similar to search_meta
```json
{
    "url": "string",
    "html": "byte (brotli)",
    "content": "string",
    "topN": "string",
    "titleMatch": "string",
    "similarity": "float (-1 means no search result found)",
    "searched_url": "string, urls that has highest simi"
}
```

### search_sanity
Search results for good pages \
Schema is same as search

### search_infer_meta
metadata for search infer rules inspection\
Schema is same as search_meta


### search_infer
#### Index: from
Searched results for search_infer_meta
Schema is same as search

### search_infer_guess
urls infered by rules from searched urls for urls cannot find copies
```json
{
    "url": "string",
    "from": "string",
    "status": "2xx|45xx",
    "html": "byte (if status 2xx)",
    "content": "string (if status 2xx)"
}
```

### wappaplyzer_sanity
URLs are no redirection non-broken urls
```json
{
    "_id": "crawl url (wayback / realweb)",
    "url": "belong to url",
    "year": "int",
    "tech": "dict",
}
```

### wappalyzer_reorg
URLs are ones with copies and no DNSError
```json
{
    "_id": "crawl url (wayback / realweb copy)",
    "url": "belong to url",
    "year": "int",
    "tech": "dict",
}
```

### search_sanity_prefix
Prefix for urls where search engine can get prefix search
```json
{
    "_id": "url",
    "url": "url",
    "google_dir": "dirname find results",
    "google_urls": "list",
    "bing_dir": "dirname find results",
    "bing_urls": "list", 
}
```

### site_tech
Sample 10k subhosts which has broken pages 
```json
{
    "_id": "subhost",
    "subhost": "subhost",
    "hostname": "sitename",
    "techs": [{
        "startTS": "str",
        "endTS": "str",
        "tech": {}
    }]
}
```

### site_url_before_after
Collect urls appear before and after site's tech has changed/same\
Index on subhost
```json
{
    "url": "url",
    "type": "Change | Same",
    "subhost": "subhost",
    "beforeTS": "str",
    "afterTS": "str",
    "periodID": "int",
    "afterStatus": "str",
    "beforeTech": {},
    "afterTech" {}
}
```
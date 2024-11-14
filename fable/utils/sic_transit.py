"""
Implementation of detection of broken pages from sic transit 
"""
from lib2to3.pgen2 import token
import requests
from urllib.request import urlopen, Request
import re
import os
from urllib.parse import urlparse, parse_qsl, urlsplit, urlunsplit
import random, string
import sys
from math import ceil
from bs4 import BeautifulSoup

from fable import config
from . import text_utils, url_utils, crawl
from .crawl import rp 
import logging
logger = logging.getLogger('logger')

sys.setrecursionlimit(1500)
he = url_utils.HostExtractor()

def alternative_request(url, timeout=15):
    httprequest = Request(url, headers={"user-agent": config.config('user_agent')})
    with urlopen(httprequest, timeout=timeout) as response:
        r = requests.Response()
        r.status_code = response.status
        if isinstance(response.url, bytes):
            r.url = response.url.decode('utf-8')
        else:
            r.url = response.url
        r.headers = dict(response.headers)
        r._content = response.read()
        return r

def send_request(url, timeout=15):
    """Only fetch for response body when content-type is HTML"""
    resp = None
    requests_header = {'user-agent': config.config('user_agent')}

    req_failed = True
    if not rp.allowed(url, requests_header['user-agent']):
        return None, 'Not Allowed'
    try:
        resp = requests.get(url, headers=requests_header, timeout=timeout, stream=True)
        # resp = requests.get(url, headers=requests_header, timeout=timeout, stream=True, verify=False)
        headers = {k.lower(): v.lower() for k, v in resp.headers.items()}
        content_type = headers['content-type'] if 'content-type' in headers else ''
        if 'html' in content_type:
            content = resp.content
        req_failed = False
    # Requsts timeout
    except requests.exceptions.ReadTimeout:
        error_msg = 'ReadTimeout'
    except requests.exceptions.Timeout:
        error_msg = 'Timeout'
    # DNS Error or tls certificate verify failed
    except requests.exceptions.ConnectionError as exc:
        reason = str(exc)
        # after looking for the failure info, the following should be the errno for DNS errors.
        if ("[Errno 11001] getaddrinfo failed" in reason or     # Windows
            "[Errno -2] Name or service not known" in reason or # Linux
            "[Errno 8] nodename nor servname " in reason):      # OS X
            error_msg = 'ConnectionError_DNSLookupError'
        else:
            error_msg = 'ConnectionError'
    except requests.exceptions.MissingSchema:
        error_msg = 'MissingSchema'
    except requests.exceptions.InvalidSchema:
        error_msg = 'InvalidSchema'
    except requests.exceptions.TooManyRedirects:
        try:
            resp = alternative_request(url)
            req_failed = False
        except:
            error_msg = 'TooManyRedirects'
    except requests.exceptions.RequestException:
        error_msg = 'RequestException'
    except UnicodeError:
        error_msg = 'ERROR_UNICODE'
    except Exception as _:
        error_msg = 'ERROR_REQUEST_EXCEPTION_OCCURRED'

    if req_failed:
        return resp, error_msg

    return resp, 'SUCCESSFUL'


def get_status(url, resp, msg):
    status, detail = "", ""
    if msg == 'SUCCESSFUL':
        final_url, status_code = resp.url, resp.status_code
        url_path = urlparse(url).path
        final_url_path = urlparse(final_url).path
        # remove the last '/' if it exists
        if url_path.endswith('/'):
            url_path = url_path[:-1]
        if final_url_path.endswith('/'):
            final_url_path = final_url_path[:-1]
        
        status = str(status_code)
        # if the response status code is 400 or 500 level, brokem
        if int(status_code / 100) >= 4:
            detail = status_code
        # if status code is 200 level and no redirection
        elif (int(status_code/100) == 2 or int(status_code/100) == 3) and final_url_path == url_path:
            detail = 'no redirection'
        # if a non-hompage redirects to a homepage, considered broken
        elif final_url_path == '' and url_path != '':
            detail = 'homepage redirection'
        # if it redirects to another path, we are unsure.
        elif final_url_path != url_path:
            detail = 'non-home redirection'

        # do not know what redirection happens
        else:
            # this list should be empty
            detail = 'unknown redirection'
    else:
        if 'ConnectionError_DNSLookupError' in msg:
            status = 'DNSError'
        elif msg == 'TooManyRedirects':
            status = 'OtherError'
            detail = 'TooManyRedirects'
        else:
            status = 'OtherError'
            detail = 'othererror'
            if "DNS" in detail: status = "DNSError"
    return status, detail


def construct_rand_urls(url):
    """
    Construct random urls from given url. Randomed part satisfies:
        With same format. Consists of the same char format as old ones
        Return urls with all possible random construction
    """
    random_urls = []
    up = urlparse(url)
    def similar_pattern(name):
        sep = "$-_.+!*'(),"
        lower_char = [c for c in name if c.islower()]
        upper_char = [c for c in name if c.isupper()]
        num_char = [c for c in name if c.isdigit()]
        if (len(lower_char) + len(upper_char) + len(num_char)) == 0:
            return ''.join([random.choice(string.ascii_letters) for _ in range(25)])
        else: 
            ratio = ceil(25/(len(lower_char) + len(upper_char) + len(num_char)))
            for c in lower_char:
                name = name.replace(c, ''.join([random.choice(string.ascii_lowercase) for _ in range(ratio)]))
            for c in upper_char:
                name = name.replace(c, ''.join([random.choice(string.ascii_uppercase) for _ in range(ratio)]))
            for c in num_char:
                name = name.replace(c, ''.join([random.choice(string.digits) for _ in range(ratio)]))
            return name
    scheme, netloc, path, query = up.scheme, up.netloc, up.path, up.query
    end_with_slash = False
    if path == '': path += '/'
    elif path != '/' and path[-1] == '/': 
        end_with_slash = True
        path = path[:-1]
    # Filename Random construction
    url_dir, filename = os.path.dirname(path), os.path.basename(path)
    # * Keep the same file ext
    filename, ext = os.path.splitext(filename)
    random_filename = similar_pattern(filename) + ext
    random_url = f"{scheme}://{netloc}{os.path.join(url_dir, random_filename)}"
    if end_with_slash: random_url += '/'
    if query: random_url += '?' + query
    random_urls.append(random_url)
    # Query Random construct
    if not query: return random_urls
    ql = parse_qsl(query)
    if len(ql) == 0: # Not valid query string. Replace all together
        q = similar_pattern(query)
        random_url = f"{scheme}://{netloc}{path}"
        if end_with_slash: random_url += '/'
        random_url += '?' + q
        random_urls.append(random_url)
    else:
        random_urls = []
        for idx, qkv in enumerate(ql):
            qv = similar_pattern(qkv[1])
            query_cp = ql.copy()
            query_cp[idx] = (qkv[0], qv)
            rand_query = '&'.join([f'{q[0]}={q[1]}' for q in query_cp])
            random_url = f"{scheme}://{netloc}{path}"
            if end_with_slash: random_url += '/'
            random_url += '?' + rand_query
            random_urls.append(random_url)
    return random_urls

def change_url_digit(url):
    """
    Detect any parts of path in the middle, if all digits, change with others
    Returns: Applicable urls
    """
    import math
    us = urlsplit(url)
    path = us.path
    if path == '': path = '/'
    parts = path.split('/')
    pos_rpr = []
    for i in range(len(parts)):
        if not parts[i].isnumeric(): continue
        part_d = int(parts[i])
        base_d = 10 ** int(math.log10(part_d)) if part_d > 0 else 0
        if part_d <= base_d: part_d = base_d + 1
        another_d = random.randrange(base_d, part_d)
        new_parts = parts.copy()
        new_parts[i] = str(another_d)
        pos_rpr.append(new_parts)
    return [urlunsplit(us._replace(path='/'.join(pr))) for pr in pos_rpr]

def filter_redir(r):
    """Filter out simple redirections from http --> https"""
    old_his = [h.url for h in r.history] + [r.url]
    new_his = []
    for idx, (h_bef, h_aft) in enumerate(zip(old_his[:-1], old_his[1:])):
        if not h_bef.split('://')[-1] == h_aft.split('://')[-1]:
            new_his.append(r.history[idx])
    return new_his

def text_norm(text):
    filter_char = ' \n\t'
    text = re.sub(f'[{filter_char}]+', ' ', text)
    return text


def broken(url, html=False, ignore_soft_404=False, ignore_soft_404_content=False,
            redir_home=False):
    """
    Entry func: detect whether this url is broken
    html: Require the url to be html.
    ignore_soft_404: Whether soft-404 detection will be ignored
    ignore_soft_404_content: Ignore only content comparison soft-404
    redir_home: If the redir is non-home page to homepage: consider wrong

    Return: True/False/"N/A", reason
    """
    resp, msg = send_request(url)
    if msg == 'Not Allowed':
        return 'N/A', msg
    status, _ = get_status(url, resp, msg)
    if re.compile('^([45]|DNSError|OtherError)').match(status):
        return True, status
    headers = {k.lower(): v for k, v in resp.headers.items()}
    content_type = headers['content-type'] if 'content-type' in headers else ''
    if html and 'html' not in content_type:
        logger.info('sic transit broken: Not HTML')
        return "N/A", "Not html"
    elif 'html' not in content_type: # * Not HTML, not detecting soft-404 on Non HTML resource
        return True, "Non soft-404 on non-HTML resource"
    if ignore_soft_404:
        return False, "No hard broken"
    # Ignore Homepages
    if urlsplit(url).path in ['', '/']:
        return False, "Homepage (no Soft-404 detection)"
    final_url = crawl.get_canonical(url, resp.text)
    # Non home to home 
    if redir_home:
        if urlsplit(url).path not in ['', '/'] and urlsplit(final_url).path in ['', '/']:
            return True, "Non homepage to homepage"
    try:
        soup = BeautifulSoup(resp.text, 'lxml')
        if len(soup.find_all('link', {'rel': 'canonical'})) > 0:
            if urlsplit(resp.url).path in ['', '/']: raise
            if url_utils.url_match(url, resp.url):
                return False, "With Canonical"
            # site = he.extract(url)
            # hp, _ = send_request(f'http://{site}')
            # if not url_utils.url_match(hp.url, resp.url):
            #     return False, "With Canonical"
    except: pass
    # Construct new url with random filename
    random_urls = construct_rand_urls(url)
    random_urls += change_url_digit(url)
    broken_decision, reasons = [], []
    for random_url in random_urls:
        # print(random_url)
        # * If original request no timeout issue, so should be this one
        random_resp, msg = send_request(random_url, timeout=15)
        if msg == 'Not Allowed':
            continue
        random_status, _ = get_status(random_url, random_resp, msg)
        if re.compile('^([45]|DNSError|OtherError)').match(random_status):
            broken_decision.append(False)
            reasons.append("random url hard broken")
            break
        # * Filter out http --> https redirection
        # ? Only consider if both has redirection (even drop this?)
        if len(filter_redir(resp)) != len(filter_redir(random_resp)) and len(filter_redir(resp)) > 0:
            broken_decision.append(False)
            reasons.append("#redirection doesn't match")
            break
        if resp.url == random_resp.url:
            broken_decision.append(True)
            reasons.append("Same final url")
            continue
        # url_content = text_utils.extract_body(resp.text, version='domdistiller')
        # random_content = text_utils.extract_body(random_resp.text, version='domdistiller')
        # * Content soft-404 comparison
        if not ignore_soft_404_content:
            try:
                url_content = BeautifulSoup(resp.text, 'lxml').get_text(separator=' ')
            except Exception as e:
                url_content = resp.text
            # ? Try to filter case for js oriented page loading
            tokenized_content = text_norm(url_content).split(' ')
            broken_keywords = ['login', 'subscription', 'error', 'notfound', '404', 'badpage', 'not found']
            if len(tokenized_content) < 10:
                for k in broken_keywords:
                    if k in url_content:
                        broken_decision.append(True)
                        reasons.append("short content with broken keywords")
                        return True, reasons
                broken_decision.append(False)
                reasons.append("no features match")
                continue
            try:
                random_content = BeautifulSoup(random_resp.text, 'lxml').get_text(separator=' ')
            except: random_content = random_resp.text
            if text_utils.k_shingling(text_norm(url_content), text_norm(random_content)) >= 0.9:
                # print(text_norm(url_content), text_norm(random_content))
                broken_decision.append(True)
                reasons.append("Similar soft 404 content")
                continue
        broken_decision.append(False)
        reasons.append("no features match")
        break
    if len(reasons) == 0:
        return 'N/A', 'Guess URLs not allowed'
    else:
        return not False in broken_decision, reasons
    



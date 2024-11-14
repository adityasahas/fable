import json
import logging
import argparse
from fable import tools, fable, tracer, config, verifier
from fable.utils import url_utils
import os

parser = argparse.ArgumentParser()
parser.add_argument('--log', help='path of execution log, in .log ext', default='fable_run.log')
parser.add_argument('input_file', nargs=1, help='Input file for broken links (in json format)')
parser.add_argument('output_file', nargs=1, help='Output file for found aliases (in json format)')
args = parser.parse_args()

log_file = '.'.join(args.log.split('.')[:-1])
input_file = args.input_file[0]
output_file = args.output_file[0]

he = url_utils.HostExtractor()
memo = tools.Memoizer()
simi = None
db = config.DB
alias_finder = None
tr = None

def _init_large_obj():
    global simi, alias_finder, tr
    if tr is None:
        try:
            os.remove(log_file + '.log')
        except: pass
    if simi is None:
        simi = tools.Similar()
    if alias_finder is None:
        alias_finder = fable.AliasFinder(similar=simi, classname=log_file, loglevel=logging.DEBUG)

_init_large_obj()
input_urls = json.load(open(input_file, 'r'))
results = []
for obj in input_urls:
    netloc = obj['netloc_dir']
    urls = obj['urls']
    aliases = alias_finder.run_order(netloc, urls)
    results.append({
        'netloc_dir': netloc,
        'aliases': aliases
    })
    json.dump(results, open(output_file, 'w+'), indent=2)
json.dump(results, open(output_file, 'w+'), indent=2)

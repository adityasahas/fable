from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict
from fable import tools, fable
from fable.utils import url_utils
import logging
import os

app = FastAPI()


class URLInput(BaseModel):
    netloc_dir: str
    urls: List[str]


class AliasResponse(BaseModel):
    netloc_dir: str
    aliases: List[Dict]


he = url_utils.HostExtractor()
memo = tools.Memoizer()
simi = None
alias_finder = None
tr = None


def init_large_obj(log_file="fable_run"):
    global simi, alias_finder, tr
    if tr is None:
        try:
            os.remove(log_file + ".log")
        except Exception:
            pass
    if simi is None:
        simi = tools.Similar()
    if alias_finder is None:
        alias_finder = fable.AliasFinder(
            similar=simi, classname=log_file, loglevel=logging.DEBUG
        )


@app.on_event("startup")
async def startup_event():
    init_large_obj()


@app.post("/find_aliases", response_model=List[AliasResponse])
async def find_aliases(input_urls: List[URLInput]):
    try:
        results = []
        for obj in input_urls:
            aliases = alias_finder.run_order(obj.netloc_dir, obj.urls)
            results.append({"netloc_dir": obj.netloc_dir, "aliases": aliases})
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health_check():
    return {"status": "healthy"}

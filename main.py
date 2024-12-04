from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict
from fable import tools, fable
from fable.utils import url_utils
import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)
logger = logging.getLogger(__name__)

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
        except Exception as e:
            logger.warning(f"Could not remove log file: {e}")
            pass
    if simi is None:
        logger.info("Initializing Similar object")
        simi = tools.Similar()
    if alias_finder is None:
        logger.info("Initializing AliasFinder")
        alias_finder = fable.AliasFinder(
            similar=simi, classname=log_file, loglevel=logging.DEBUG
        )


@app.on_event("startup")
async def startup_event():
    logger.info("Starting up application")
    init_large_obj()
    logger.info("Finished startup initialization")


@app.post("/find_aliases")
async def find_aliases(input_urls: List[URLInput]):
    try:
        logger.info("Finding aliases - starting processing")
        results = []
        total = len(input_urls)

        for idx, obj in enumerate(input_urls, 1):
            logger.info(f"Processing {idx}/{total}: {obj.netloc_dir}")
            aliases = alias_finder.run_order(obj.netloc_dir, obj.urls)
            logger.info(f"Completed {idx}/{total}: {obj.netloc_dir}")
            results.append({"netloc_dir": obj.netloc_dir, "aliases": aliases})

        return results
    except Exception as e:
        logger.error(f"Error in find_aliases: {str(e)}")
        import traceback

        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health_check():
    logger.info("Health check requested")
    return {"status": "healthy"}

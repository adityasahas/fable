from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import List, Dict, Optional, Tuple
from fable import tools, fable, config
from fable.utils import url_utils
from tests.test_utils import test_domdistiller

import logging
import os
import sys
from datetime import datetime
import uuid
import json
import asyncio

os.makedirs("logs", exist_ok=True)
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = f"logs/fable_api_{timestamp}.txt"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_filename, mode="a"),
    ],
    force=True,
)
logger = logging.getLogger("fable-api")

app = FastAPI()


class URLInput(BaseModel):
    netloc_dir: str
    urls: List[str]


class AliasResponse(BaseModel):
    netloc_dir: str
    aliases: List[Dict]


class TaskResponse(BaseModel):
    task_id: str
    status: str
    created_at: str


class TaskStatus(BaseModel):
    task_id: str
    status: str
    created_at: str
    completed_at: Optional[str] = None
    result: Optional[List[Dict]] = None
    error: Optional[str] = None


class TaskStore:
    def __init__(self):
        self.db = config.DB 
        self.collection = self.db.tasks  

        self.collection.create_index("status")
        self.collection.create_index("created_at")

    def create_task(self) -> str:
        task_id = str(uuid.uuid4())
        task = {
            "_id": task_id,
            "status": "pending",
            "created_at": datetime.utcnow().isoformat(),
            "completed_at": None,
            "result": None,
            "error": None,
        }
        self.collection.insert_one(task)
        return task_id

    def update_task(self, task_id: str, **kwargs):
        self.collection.update_one({"_id": task_id}, {"$set": kwargs})

    def get_task(self, task_id: str) -> Optional[Dict]:
        task = self.collection.find_one({"_id": task_id})
        return task if task else None

    async def find_existing_results(
        self, netloc_dir: str, urls: List[str]
    ) -> Tuple[List[str], Dict[str, dict]]:
        """
        Search through completed tasks for matching URLs
        Returns: (urls_to_process, cached_results)
        """
        try:
            urls_set = set(urls)
            cached_results = {}

            completed_tasks = self.collection.find({"status": "completed"})

            for task in completed_tasks:
                if task.get("result"):
                    for result in task["result"]:
                        if result["netloc_dir"] == netloc_dir:
                            for alias in result["aliases"]:
                                source_url = alias["source_url"]
                                if source_url in urls_set:
                                    cached_results[source_url] = alias
                                    urls_set.remove(source_url)

            return list(urls_set), cached_results
        except Exception as e:
            logger.error(f"Error searching existing results: {str(e)}")
            return urls, {}


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


task_store = TaskStore()


async def process_aliases_task(task_id: str, input_urls: List[URLInput]):
    try:
        results = []
        total = len(input_urls)

        for idx, obj in enumerate(input_urls, 1):
            logger.info(f"Processing {idx}/{total}: {obj.netloc_dir}")

            urls_to_process, cached_results = await task_store.find_existing_results(
                obj.netloc_dir, obj.urls
            )

            final_aliases = []

            if cached_results:
                logger.info(
                    f"Found {len(cached_results)} cached results for {obj.netloc_dir}"
                )
                final_aliases.extend(cached_results.values())

            if urls_to_process:
                logger.info(
                    f"Processing {len(urls_to_process)} new URLs for {obj.netloc_dir}"
                )
                loop = asyncio.get_event_loop()
                new_aliases = await loop.run_in_executor(
                    None, alias_finder.run_order, obj.netloc_dir, urls_to_process
                )

                for alias_group in new_aliases:
                    formatted_alias = {
                        "source_url": alias_group[0],
                        "titles": list(alias_group[1]),
                        "target_url": alias_group[2],
                        "match_types": list(alias_group[3]),
                    }
                    final_aliases.append(formatted_alias)

                logger.info(f"Completed processing new URLs for {obj.netloc_dir}")

            results.append({"netloc_dir": obj.netloc_dir, "aliases": final_aliases})

        task_store.update_task(
            task_id,
            status="completed",
            completed_at=datetime.utcnow().isoformat(),
            result=results,
        )

    except Exception as e:
        logger.error(f"Error in task {task_id}: {str(e)}")
        task_store.update_task(
            task_id,
            status="failed",
            completed_at=datetime.utcnow().isoformat(),
            error=str(e),
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

            urls_to_process, cached_results = await task_store.find_existing_results(
                obj.netloc_dir, obj.urls
            )

            final_aliases = []

            if cached_results:
                logger.info(
                    f"Found {len(cached_results)} cached results for {obj.netloc_dir}"
                )
                for alias in cached_results.values():
                    final_aliases.append(
                        [
                            alias["source_url"],
                            alias["titles"],
                            alias["target_url"],
                            alias["match_types"],
                        ]
                    )

            if urls_to_process:
                logger.info(
                    f"Processing {len(urls_to_process)} new URLs for {obj.netloc_dir}"
                )
                new_aliases = alias_finder.run_order(obj.netloc_dir, urls_to_process)
                final_aliases.extend(new_aliases)

                formatted_aliases = []
                for alias_group in new_aliases:
                    formatted_alias = {
                        "source_url": alias_group[0],
                        "titles": list(alias_group[1]),
                        "target_url": alias_group[2],
                        "match_types": list(alias_group[3]),
                    }
                    formatted_aliases.append(formatted_alias)

                task_id = task_store.create_task()
                task_store.update_task(
                    task_id,
                    status="completed",
                    completed_at=datetime.utcnow().isoformat(),
                    result=[
                        {"netloc_dir": obj.netloc_dir, "aliases": formatted_aliases}
                    ],
                )

            results.append({"netloc_dir": obj.netloc_dir, "aliases": final_aliases})

        return results
    except Exception as e:
        logger.error(f"Error in find_aliases: {str(e)}")
        import traceback

        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/start_alias_task", response_model=TaskResponse)
async def start_alias_task(
    input_urls: List[URLInput], background_tasks: BackgroundTasks
):
    logger.info("Starting new alias task")

    all_cached_results = []
    all_cached = True

    for obj in input_urls:
        urls_to_process, cached_results = await task_store.find_existing_results(
            obj.netloc_dir, obj.urls
        )
        if urls_to_process:
            all_cached = False
            break
        all_cached_results.append(
            {"netloc_dir": obj.netloc_dir, "aliases": list(cached_results.values())}
        )

    if all_cached:
        task_id = task_store.create_task()
        task_store.update_task(
            task_id,
            status="completed",
            completed_at=datetime.utcnow().isoformat(),
            result=all_cached_results,
        )
        return {
            "task_id": task_id,
            "status": "completed",
            "created_at": task_store.get_task(task_id)["created_at"],
        }

    task_id = task_store.create_task()
    logger.info(f"Created task ID: {task_id}")

    background_tasks.add_task(process_aliases_task, task_id, input_urls)

    response = {
        "task_id": task_id,
        "status": "pending",
        "created_at": task_store.get_task(task_id)["created_at"],
    }

    logger.info(f"Added task {task_id} to background processing")
    return response


@app.get("/task_status/{task_id}", response_model=TaskStatus)
async def get_task_status(task_id: str):
    task = task_store.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    return {"task_id": task_id, **task}


@app.get("/health")
async def health_check():
    logger.info("Health check requested")
    return {"status": "healthy"}

@app.get("/test_domdistiller")
async def test_domdistiller_endpoint():
    title, content = test_domdistiller()
    return {"title": title, "content": content}
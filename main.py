from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import List, Dict, Optional
from fable import tools, fable
from fable.utils import url_utils
import logging
import os
import sys
from datetime import datetime
import uuid
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
        self.tasks: Dict[str, Dict] = {}

    def create_task(self) -> str:
        task_id = str(uuid.uuid4())
        self.tasks[task_id] = {
            "status": "pending",
            "created_at": datetime.utcnow().isoformat(),
            "completed_at": None,
            "result": None,
            "error": None,
        }
        return task_id

    def update_task(self, task_id: str, **kwargs):
        if task_id in self.tasks:
            self.tasks[task_id].update(kwargs)

    def get_task(self, task_id: str) -> Optional[Dict]:
        return self.tasks.get(task_id)


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
            loop = asyncio.get_event_loop()
            aliases = await loop.run_in_executor(
                None, alias_finder.run_order, obj.netloc_dir, obj.urls
            )
            logger.info(f"Completed {idx}/{total}: {obj.netloc_dir}")
            results.append({"netloc_dir": obj.netloc_dir, "aliases": aliases})

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
            aliases = alias_finder.run_order(obj.netloc_dir, obj.urls)
            logger.info(f"Completed {idx}/{total}: {obj.netloc_dir}")
            results.append({"netloc_dir": obj.netloc_dir, "aliases": aliases})

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
    task_id = task_store.create_task()
    logger.info(f"Created task ID: {task_id}")

    response = {
        "task_id": task_id,
        "status": "pending",
        "created_at": task_store.tasks[task_id]["created_at"],
    }

    background_tasks.add_task(process_aliases_task, task_id, input_urls)
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

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from typing import List, Dict
import json
from fable import tools, fable, tracer, config
from fable.utils import url_utils
import logging
import os
import uuid
from google.cloud import pubsub_v1, firestore

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
db = config.DB
alias_finder = None
tr = None

firestore_client = firestore.Client()

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path("google-cloud-project-id", "task-topic")


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


@app.post("/find_aliases", response_model=Dict)
async def find_aliases(input_urls: List[URLInput]):
    # input urls schema: [{"netloc_dir": "example.com", "urls": ["url1", "url2"]}]
    task_ids = []
    try:
        for obj in input_urls:
            task_id = str(uuid.uuid4())
            task_data = {
                "task_id": task_id,
                "netloc_dir": obj.netloc_dir,
                "urls": obj.urls,
                "status": "in_progress",
            }

            publisher.publish(topic_path, json.dumps(task_data).encode("utf-8"))

            firestore_client.collection("tasks").document(task_id).set(task_data)

            task_ids.append(task_id)

        return {"task_ids": task_ids}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/check_task/{task_id}")
async def check_task(task_id: str):
    task_doc = firestore_client.collection("tasks").document(task_id).get()
    if task_doc.exists:
        return task_doc.to_dict()
    else:
        raise HTTPException(status_code=404, detail="Task not found")


def callback(message):
    try:
        data = json.loads(message.data)
        task_id = data["task_id"]
        netloc_dir = data["netloc_dir"]
        urls = data["urls"]

        aliases = alias_finder.run_order(netloc_dir, urls)

        firestore_client.collection("tasks").document(task_id).update(
            {"status": "completed", "aliases": aliases}
        )

        message.ack()
    except Exception as e:
        firestore_client.collection("tasks").document(task_id).update(
            {"status": "failed", "error": str(e)}
        )
        message.ack()


subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(
    "your-google-cloud-project-id", "task-subscription"
)
subscriber.subscribe(subscription_path, callback=callback)


@app.get("/health")
async def health_check():
    return {"status": "healthy"}

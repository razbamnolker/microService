import uuid
import pika
import datetime
import uvicorn
import logging
import os
from pymongo import MongoClient
from typing import Any, Mapping
from fastapi import FastAPI
from utils.MessageFormatter import MessageFormatter
from utils.Status import Status
from configparser import ConfigParser


class API:
    _CONFIG_FILE_PATH = os.path.join(
        os.path.dirname(__file__), '..', 'Configuration.ini'
    )
    _CONFIG = ConfigParser()
    _CONFIG.read(_CONFIG_FILE_PATH)
    # DB related fields
    _MONGO_CONNECTION_STRING = _CONFIG['Mongo']['_MONGO_CONNECTION_STRING']
    _DB_NAME = _CONFIG['Mongo']['_DB_NAME']
    _COLLECTION_NAME = _CONFIG['Mongo']['_COLLECTION_NAME']
    _CLIENT = MongoClient(_MONGO_CONNECTION_STRING)
    _DB = _CLIENT[_DB_NAME]
    _COLLECTION = _DB[_COLLECTION_NAME]

    # RabbitMQ related fields
    _RABBIT_CONNECTION = pika.BlockingConnection(pika.ConnectionParameters(
                host=_CONFIG['RabbitMQ']['_RABBIT_HOST'],
                port=_CONFIG['RabbitMQ']['_RABBIT_PORT'],
                virtual_host='/',
                credentials=pika.PlainCredentials('guest', 'guest')
            ))
    _CHANNEL = _RABBIT_CONNECTION.channel()
    _RABBIT_QUEUE_NAME = _CONFIG['RabbitMQ']['_RABBIT_QUEUE_NAME']

    def __init__(self):
        self.app = FastAPI()
        self.setup_routes()
        logging.basicConfig(filename="api_logger.txt", encoding='utf-8', level=logging.INFO)

    def setup_routes(self):
        @self.app.get("/job/{job_id}")
        def query_job(job_id: str) -> Mapping[str, Any]:
            """

            :param job_id: The ID of the job we want to look for
            :return: All details of job if exist , else 404
            """
            job = API._COLLECTION.find_one({"_id": job_id})
            if job is None:
                logging.info(f"Executed get/{job_id} and got no result.")
                return {"404": f"Could not find a job with the given ID:{job_id}"}
            logging.info(f"Executed get/{job_id} and got result successfully.")
            return job

        @self.app.get("/fbid/{username}")
        def query_username(username: str) -> dict:
            """

            :param username:The username who we would like to get his fb_id
            :return: fb_id if exist for this username , else 404
            """
            # We want the newest result for the username
            job = API._COLLECTION.find({"username": username}).sort({"end_time": -1}).limit(1)
            try:
                fb_id = job.next()['fb_id']
                logging.info(f"Executed fbid/{username} and got result successfully.")
                return {"fb_id": fb_id}
            except StopIteration:
                logging.info(f"Executed fbid/{username} and got no result.")
                return {"404": f"Could not find a result for the following username:{username}"}

        @self.app.post("/post_job/{username}")
        def create_job(username: str) -> dict:
            """
            Creating a new job , adding it to the db and sending a message using rabbitMQ to the microservice
            :param username:
            """
            job_id = str(uuid.uuid4())
            job = {"_id": job_id, "username": username, "start_time": datetime.datetime.now(),
                   "status": Status.Ready.name}
            API._COLLECTION.insert_one(job)
            logging.info(f"Created a new job successfully. job_id:{job_id} ")
            msg_body = MessageFormatter().encode_msg(username, job_id)
            API._CHANNEL.basic_publish(exchange='', routing_key='send_jobs', body=msg_body)
            return {"msg": f"Received successfully , your job_id is:{job_id}"}

    def run(self):
        uvicorn.run(self.app, host="127.0.0.1", port=8000)


if __name__ == '__main__':
    API().run()

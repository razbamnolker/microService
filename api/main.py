from typing import Any, Mapping
from fastapi import FastAPI
import pika
from pymongo import MongoClient
import datetime
import uvicorn
from utils.Status import Status
from utils.MessageFormatter import MessageFormatter
import uuid


class API:
    def __init__(self):
        self.app = FastAPI()
        # RabbitMQ fields
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(
                host='localhost',
                port=5672,
                virtual_host='/',
                credentials=pika.PlainCredentials('guest', 'guest')
            ))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='send_jobs')  # Sending the created jobs to the microservice
        # DB Related Fields
        self.connection_string = "mongodb://localhost:27017/"
        self.db_name = "Jobs"
        self.client = MongoClient(self.connection_string)
        self.db = self.client[self.db_name]
        self.collection = self.db["jobs"]
        self.setup_routes()

    def setup_routes(self):
        @self.app.get("/job/{job_id}")
        def query_job(job_id: str) -> Mapping[str, Any]:
            """

            :param job_id: The ID of the job we want to look for
            :return: All details of job if exist , else 404
            """
            job = self.collection.find_one({"_id": job_id})
            if job is None:
                return {"404": f"Could not find a job with the given ID:{job_id}"}
            return job

        @self.app.get("/fbid/{username}")
        def query_username(username: str) -> dict:
            """

            :param username:The username who we would like to get his fb_id
            :return: fb_id if exist for this username , else 404
            """
            # We want the newest result for the username
            job = self.collection.find({"username": username}).sort({"end_time": -1}).limit(1)
            if job is None:
                return {"404": f"Could not find a result for the following username:{username}"}
            return {"fb_id": job[0]['fb_id']}

        @self.app.post("/post_job/{username}")
        def create_job(username: str) -> dict:
            """
            Creating a new job , adding it to the db and sending a message using rabbitMQ to the microservice
            :param username:
            """
            job_id = str(uuid.uuid4())
            job = {"_id": job_id, "username": username, "start_time": datetime.datetime.now(),
                   "status": Status.Ready.name}
            self.collection.insert_one(job)
            msg_body = MessageFormatter().encode_msg(username, job_id)
            self.channel.basic_publish(exchange='', routing_key='send_jobs', body=msg_body)
            return {"msg": f"Received successfully , your job_id is:{job_id}"}

    def run(self):
        uvicorn.run(self.app, host="127.0.0.1", port=8000)


if __name__ == '__main__':
    API().run()


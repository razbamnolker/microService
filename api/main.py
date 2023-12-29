from fastapi import FastAPI
import pika
from pymongo import MongoClient
import datetime

app = FastAPI()
# RabbitMQ fields
connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost',
        port=5672,
        virtual_host='/',
        credentials=pika.PlainCredentials('guest', 'guest')
    ))
channel = connection.channel()
channel.queue_declare(queue='send_jobs')  # Sending the created jobs to the microservice
# DB Related Fields
next_job_id = 1
connection_string = "mongodb://localhost:27017/"
db_name = "Jobs"
client = MongoClient(connection_string)
db = client[db_name]
collection = db["jobs"]


@app.get("/job/{job_id}")
def query_job(job_id: str):
    """

    :param job_id: The ID of the job we want to look for
    :return: All details of job if exist , else 404
    """
    job = collection.find_one({"_id": job_id})
    if job is None:
        return {"404": f"Could not find a job with the given ID:{job_id}"}
    return job


@app.get("/fbid/{username}")
def query_username(username: str):
    """

    :param username:The username who we would like to get his fb_id
    :return: fb_id if exist for this username , else 404
    """
    query = {"username": username}
    # We want the newest result for the username
    job = collection.find_one()(query).sort({"end_time": -1}).limit(1)
    if job is None:
        return {"404": f"Could not find a result for the following username:{username}"}
    return {"fb_id": job['fb_id']}


@app.post("/job/{username}")
def create_job(username: str):
    """
    Creating a new job , adding it to the db and sending a message using rabbitMQ to the microservice
    :param username:
    """
    global next_job_id
    job = {"_id": next_job_id, "username": username, "start_time": datetime.datetime.now(), "status": "Ready"}
    collection.insert_one(job)
    msg_body = f"{username},{next_job_id}"
    channel.basic_publish(exchange='', routing_key='send_jobs', body=msg_body)
    next_job_id += 1

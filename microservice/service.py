import datetime
import re
import requests
import pika
from pymongo import MongoClient


class Service:
    def __init__(self):
        # DB related fields
        self.connection_string = "mongodb://localhost:27017/"
        self.db_name = "Jobs"
        self.client = MongoClient(self.connection_string)
        self.db = self.client[self.db_name]
        self.collection = self.db["jobs"]
        # RabbitMQ related fields
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(
            host='localhost',
            port=5672,
            virtual_host='/',
            credentials=pika.PlainCredentials('guest', 'guest')
        ))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='send_jobs')  # Getting the created jobs from the api

    def process_msg(self, body: bytes):
        """
        Takes a message , split to the both parts - username and job_id and then passing them to the method
            who starts the job.
        :return: Tuple of username and job_id
        :param body:(bytes) - The message received from rabbitMQ.
        """
        str_body = body.decode("utf-8")
        username, job_id = str_body.split(',')
        try:
            job_id = int(job_id)
        except ValueError as ve:
            raise ValueError(ve.args)
        return username, job_id

    def handle_msg(self):
        while True:
            self.channel.basic_consume(queue='hello', on_message_callback=self.process_msg, auto_ack=True)
            print(' [*] Waiting for messages. To exit press CTRL+C')
            self.channel.start_consuming()

    def start_job(self, body: bytes):
        username, job_id = self.process_msg(body=body)
        self.update_status_after_starting_job(job_id)
        response = requests.get(f"https://facebook.com/{username}")
        if response.ok:
            data = response.content
            decoded_string = data.decode()
            lines = decoded_string.split("\n")
            for line in lines:
                match = re.search(r'content="fb://profile/(\d+)"', line)
                if match:
                    user_id = match.group(1)
                else:
                    user_id = None
                self.update_job_after_getting_result(job_id, user_id)
                break
        # TODO what to do if response not ok

    def update_job_after_getting_result(self, job_id: int, user_id: int | None):
        if user_id is None:  # could not find username
            query = {"end_time": datetime.datetime.now(),
                     "status": "Done", "success": "False",
                     "Error_msg": "Could not find username with the given id"}
        else:
            query = {"end_time": datetime.datetime.now(),
                     "status": "Done", "fbid": user_id, "success": "True"}

        self.collection.update_one({"_id": job_id}, {"$set": query})

    def update_status_after_starting_job(self, job_id: int):
        self.collection.update_one({"_id": job_id}, {"$set": {"status": "In progress"}})

import datetime
import re
import requests
import pika
from pymongo import MongoClient
from utils.MessageFormatter import MessageFormatter
from utils.Status import Status
import logging
# TODO - lazy load rabbitMQ and Mongo
# TODO - add logger
# TODO config file


class Service:
    # DB related fields
    _CONNECTION_STRING = "mongodb://localhost:27017/"
    _DB_NAME = "Jobs"
    _COLLECTION_NAME = "jobs"
    _CLIENT = MongoClient(_CONNECTION_STRING)
    _DB = _CLIENT[_DB_NAME]
    _COLLECTION = _DB[_COLLECTION_NAME]
    # RabbitMQ related fields
    _RABBIT_CONNECTION = pika.BlockingConnection(pika.ConnectionParameters(
            host='localhost',
            port=5672,
            virtual_host='/',
            credentials=pika.PlainCredentials('guest', 'guest')
        ))
    _CHANNEL = _RABBIT_CONNECTION.channel()
    _RABBIT_QUEUE_NAME = 'send_jobs'
    _CHANNEL.queue_declare(_RABBIT_QUEUE_NAME)
    logging.basicConfig(filename='microservice_logger.txt', encoding='utf-8', level=logging.INFO)

    def handle_msg(self):
        while True:
            Service._CHANNEL.basic_consume(queue='send_jobs', on_message_callback=self.start_job, auto_ack=True)
            print(' [*] Waiting for messages. To exit press CTRL+C')
            logging.info("Started listening to messages")
            Service._CHANNEL.start_consuming()

    def start_job(self, ch, method, properties, body: bytes):
        username, job_id = MessageFormatter.decode_msg(body)
        self.update_status_after_starting_job(job_id)
        logging.info(f"Starting to execute job:{job_id} with the username :{username}")
        user_id = self.handle_request(username)
        self.update_job_after_getting_result(job_id, user_id)
        logging.info(f"Finished executing job:{job_id}")
        # TODO what to do if response not ok

    def handle_request(self, username: str):
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
                return user_id
            return None

    def update_job_after_getting_result(self, job_id: str, user_id: int | None):
        if user_id is None:  # could not find username
            query = {"end_time": datetime.datetime.now(),
                     "status": Status.Done.name, "success": "False",
                     "Error_msg": "Could not find username with the given id"}
        else:
            query = {"end_time": datetime.datetime.now(),
                     "status": Status.Done.name, "fb_id": user_id, "success": "True"}

        Service._COLLECTION.update_one({"_id": job_id}, {"$set": query})
        logging.info(f"Updated job {job_id}  status to done.")

    def update_status_after_starting_job(self, job_id: str):
        Service._COLLECTION.update_one({"_id": job_id}, {"$set": {"status": Status.InProgress.name}})
        logging.info(f"Updated job {job_id}  status to in progress")


if __name__ == '__main__':
    serv = Service()
    serv.handle_msg()

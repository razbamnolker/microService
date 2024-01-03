import datetime
import re
import requests
import pika
import logging
from pymongo import MongoClient
from utils.MessageFormatter import MessageFormatter
from utils.Status import Status
# TODO config file


class Service:
    # DB related fields
    _MONGO_CONNECTION_STRING = None
    _DB_NAME = None
    _COLLECTION_NAME = None
    _CLIENT = None
    _DB = None
    _COLLECTION = None

    # RabbitMQ related fields
    _RABBIT_CONNECTION = None
    _CHANNEL = None
    _RABBIT_QUEUE_NAME = None

    #  Logger
    logging.basicConfig(filename='microservice_logger.txt', encoding='utf-8', level=logging.INFO)

    @classmethod
    def load_mongo(cls):
        # DB related fields
        cls._MONGO_CONNECTION_STRING = "mongodb://localhost:27017/"
        cls._DB_NAME = "Jobs"
        cls._COLLECTION_NAME = "jobs"
        cls._CLIENT = MongoClient(cls._MONGO_CONNECTION_STRING)
        cls._DB = cls._CLIENT[cls._DB_NAME]
        cls._COLLECTION = cls._DB[cls._COLLECTION_NAME]

    @classmethod
    def load_rabbit(cls):
        cls._RABBIT_CONNECTION = pika.BlockingConnection(pika.ConnectionParameters(
            host='localhost',
            port=5672,
            virtual_host='/',
            credentials=pika.PlainCredentials('guest', 'guest')
        ))
        cls._CHANNEL = cls._RABBIT_CONNECTION.channel()
        cls._RABBIT_QUEUE_NAME = 'send_jobs'
        cls._CHANNEL.queue_declare(cls._RABBIT_QUEUE_NAME)

    def handle_msg(self):
        if Service._RABBIT_CONNECTION is None:
            Service.load_rabbit()
        while True:
            Service._CHANNEL.basic_consume(queue='send_jobs', on_message_callback=self.start_job, auto_ack=True)
            print(' [*] Waiting for messages. To exit press CTRL+C')
            logging.info("Started listening to messages")
            Service._CHANNEL.start_consuming()

    def start_job(self, ch, method, properties, body: bytes):
        username, job_id = MessageFormatter.decode_msg(body)
        if Service._MONGO_CONNECTION_STRING is None:
            Service.load_mongo()
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

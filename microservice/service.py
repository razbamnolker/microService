import datetime
import re
import os
import requests
import pika
import logging
from bs4 import BeautifulSoup
from pymongo import MongoClient
from microservice.utils.MessageFormatter import MessageFormatter
from microservice.utils.Status import Status
from configparser import ConfigParser


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
    config_file_path = os.path.join(
        os.path.dirname(__file__), '..', 'Configuration.ini'
    )
    _CONFIG = ConfigParser()
    _CONFIG.read(config_file_path)

    @classmethod
    def load_mongo(cls):
        # DB related fields
        cls._MONGO_CONNECTION_STRING = cls._CONFIG['Mongo']['_MONGO_CONNECTION_STRING']
        cls._DB_NAME = cls._CONFIG['Mongo']['_DB_NAME']
        cls._COLLECTION_NAME = cls._CONFIG['Mongo']['_COLLECTION_NAME']
        cls._CLIENT = MongoClient(cls._MONGO_CONNECTION_STRING)
        cls._DB = cls._CLIENT[cls._DB_NAME]
        cls._COLLECTION = cls._DB[cls._COLLECTION_NAME]

    @classmethod
    def load_rabbit(cls):
        cls._RABBIT_CONNECTION = pika.BlockingConnection(pika.ConnectionParameters(
            host=cls._CONFIG['RabbitMQ']['_RABBIT_HOST'],
            port=cls._CONFIG['RabbitMQ']['_RABBIT_PORT'],
            virtual_host='/',
            credentials=pika.PlainCredentials('guest', 'guest')
        ))
        cls._CHANNEL = cls._RABBIT_CONNECTION.channel()
        cls._RABBIT_QUEUE_NAME = cls._CONFIG['RabbitMQ']['_RABBIT_QUEUE_NAME']
        cls._CHANNEL.queue_declare(cls._RABBIT_QUEUE_NAME)

    def handle_msg(self):
        if Service._RABBIT_CONNECTION is None:
            Service.load_rabbit()
        while True:
            Service._CHANNEL.basic_consume(queue='send_jobs', on_message_callback=self.start_job, auto_ack=True)
            print(' [*] Waiting for messages. To exit press CTRL+C')
            logging.info("Started listening to messages")
            Service._CHANNEL.start_consuming()

    def start_job(self, ch, method, properties, body: bytes) -> None:
        username, job_id = MessageFormatter.decode_msg(body)
        if Service._MONGO_CONNECTION_STRING is None:
            Service.load_mongo()
        self.update_status_after_starting_job(job_id)
        logging.info(f"Starting to execute job:{job_id} with the username :{username}")
        user_id, err_msg = self.handle_request(username)
        if err_msg is None:
            self.update_job_after_getting_result(job_id, user_id)
        else:
            self.update_job_after_getting_result(job_id, user_id, err_msg)
        logging.info(f"Finished executing job:{job_id}")

    # def handle_request(self, username: str):
    #     response = requests.get(f"https://facebook.com/{username}")
    #     if response.ok:
    #         data = response.content
    #         decoded_string = data.decode()
    #         lines = decoded_string.split("\n")
    #         for line in lines:
    #             match = re.search(r'content="fb://profile/(\d+)"', line)
    #             if match:
    #                 user_id = match.group(1)
    #             else:
    #                 user_id = None
    #             return user_id
    #         return None

    def handle_request(self, username: str) -> tuple[int | None, str | None]:
        """
        Getting the url content for username-> Looking for the userID in both possible formats->
        Isolating the ID
        :param username: the username we want to get his fb_id
        :return: tuple of user_id of found else None , and error message if failed to get content else None
        """
        max_tries = int(Service._CONFIG['PARSING']['NUMBER_OF_TRIES'])
        number_of_tries = 1
        while number_of_tries <= max_tries:
            response = requests.get(f"https://facebook.com/{username}")
            if response.ok:
                soup = BeautifulSoup(response.content, 'html5lib')
                pattern = re.compile(r'fb://profile/(\d+)')
                pattern2 = re.compile(r'"userID":"(\d+)"')
                matches = pattern.findall(str(soup)) + pattern2.findall(str(soup))
                matches = set(matches)
                if len(matches):
                    user_id = set(matches).pop()
                    return int(user_id), None
                return None, None
        logging.warning(f"Failed to get the desired url for username:{username}")
        return None, f"Failed to get the desired url for username:{username}"

    def update_job_after_getting_result(self, job_id: str, user_id: int | None,
                                        err_msg: str | None = "Could not find user_id for the given username") -> None:
        """
        Updating job in db with all relevant details (end time,status,success,fb_id)
        :param job_id: job_id for the current job
        :param user_id: int if found one , else None
        :param err_msg: relevant error message if something went wrong or None.
        """
        if user_id is None:  # could not find username
            query = {"end_time": datetime.datetime.now(),
                     "status": Status.Done.name, "success": False,
                     "Error_msg": err_msg}
        else:
            query = {"end_time": datetime.datetime.now(),
                     "status": Status.Done.name, "fb_id": user_id, "success": True}

        Service._COLLECTION.update_one({"_id": job_id}, {"$set": query})
        logging.info(f"Updated job {job_id}  status to done.")

    def update_status_after_starting_job(self, job_id: str) -> None:
        """
        Update a job status from Ready to In Progress
        :param job_id:The job needed to be updated.
        """
        Service._COLLECTION.update_one({"_id": job_id}, {"$set": {"status": Status.InProgress.name}})
        logging.info(f"Updated job {job_id}  status to in progress")


if __name__ == '__main__':
    serv = Service()
    serv.handle_msg()

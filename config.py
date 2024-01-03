from configparser import ConfigParser

config = ConfigParser()

config['Mongo'] = {
        "_MONGO_CONNECTION_STRING": "mongodb://localhost:27017/",
        "_DB_NAME": "Jobs",
        "_COLLECTION_NAME": "jobs"
}

config['RabbitMQ'] = {
        "_RABBIT_HOST": 'localhost',
        "_RABBIT_PORT": 5672,
        "_RABBIT_QUEUE_NAME": 'send_jobs'
}

config['PARSING'] = {'NUMBER_OF_TRIES': 5}

with open("Configuration.ini", "w") as f:
    config.write(f)

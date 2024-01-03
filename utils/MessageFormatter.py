class MessageFormatter:
    @staticmethod
    def encode_msg(username: str, job_id: str):
        return f"{username},{job_id}"  # Extract formatting

    @staticmethod
    def decode_msg(msg: bytes):
        str_body = msg.decode("utf-8")
        username, job_id = str_body.split(',')
        return username, job_id

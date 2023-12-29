import unittest
from microservice.service import Service


class MyTestCase(unittest.TestCase):
    def setUp(self):
        self.serv = Service()

    def test_process_msg(self):
        bytes_msg: bytes = b'raz.bamnolker, 1'
        username, job_id = self.serv.process_msg(bytes_msg)
        self.assertEqual(job_id, 1)
        self.assertEqual(username,'raz.bamnolker')

    def test_process_msg_with_error(self):
        bytes_msg: bytes = b'raz.bamnolker, raz.bamnolker'
        self.assertRaises(ValueError,self.serv.process_msg(bytes_msg))


if __name__ == '__main__':
    unittest.main()

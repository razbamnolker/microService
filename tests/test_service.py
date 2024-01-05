import unittest
from microservice.service import Service


class MyTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.serv = Service()

    def test_handle_request_success(self):
        expected_res = 100064406455200, None
        actual_res = self.serv.handle_request('keshet.mako')
        self.assertEqual(actual_res, expected_res)
        actual_res = self.serv.handle_request('keshetmako')
        self.assertEqual(actual_res, expected_res)
        actual_res = self.serv.handle_request('Keshetmako')
        self.assertEqual(actual_res, expected_res)

    def test_handle_request_fail(self):
        expected_res = None, None
        self.assertEqual(self.serv.handle_request(''), expected_res)
        self.assertEqual(self.serv.handle_request('gkshgs'), expected_res)
        self.assertEqual(self.serv.handle_request('כדקמחךד'), expected_res)


if __name__ == '__main__':
    unittest.main()

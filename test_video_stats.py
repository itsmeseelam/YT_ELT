import unittest
from dags.api.video_stats import batch_list

class TestBatchList(unittest.TestCase):
    def test_batch_list(self):
        video_id_lst = [1, 2, 3, 4, 5]
        batch_size = 2
        expected_output = [[1, 2], [3, 4], [5]]
        result = list(batch_list(video_id_lst, batch_size))
        self.assertEqual(result, expected_output)

    def test_batch_list_empty(self):
        video_id_lst = []
        batch_size = 2
        expected_output = []
        result = list(batch_list(video_id_lst, batch_size))
        self.assertEqual(result, expected_output)

    def test_batch_list_single_element(self):
        video_id_lst = [1]
        batch_size = 1
        expected_output = [[1]]
        result = list(batch_list(video_id_lst, batch_size))
        self.assertEqual(result, expected_output)

    def test_batch_list_large_batch_size(self):
        video_id_lst = [1, 2, 3]
        batch_size = 10
        expected_output = [[1, 2, 3]]
        result = list(batch_list(video_id_lst, batch_size))
        self.assertEqual(result, expected_output)

if __name__ == '__main__':
    unittest.main()
import unittest
from unittest.mock import MagicMock, patch
from consumer import Consumer

class TestConsumer(unittest.TestCase):
    def setUp(self):
        self.consumer = Consumer()

    def test_createDBItem(self):
        object = {
            'widgetId': '123',
            'owner': 'John',
            'label': 'Test',
            'description': 'A test widget',
            'otherAttributes': [
                {'name': 'attr1', 'value': 'value1'},
                {'name': 'attr2', 'value': 'value2'},
            ]
        }
        dbname = 'test_db'

        item = self.consumer.createDBItem(object, dbname)

        expected_item = {
            'id': {'S': '123'},
            'owner': {'S': 'John'},
            'label': {'S': 'Test'},
            'description': {'S': 'A test widget'},
            'otherAttributes': {
                'L': [
                    {
                        'M': {
                            'name': {'S': 'attr1'},
                            'value': {'S': 'value1'}
                        }
                    },
                    {
                        'M': {
                            'name': {'S': 'attr2'},
                            'value': {'S': 'value2'}
                        }
                    }
                ]
            }
        }

        self.assertEqual(item, expected_item)

if __name__ == '__main__':
    unittest.main()
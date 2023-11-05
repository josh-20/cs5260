import argparse
import boto3
import time
import logging
import json

class Consumer:
    def __init__(self):
        logging.basicConfig(filename="consumer.log", level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.s3 = boto3.client('s3')
        self.dynamoDB = boto3.client('dynamodb', 'us-east-1')
        self.queue = boto3.client('sqs', 'us-east-1')

    def createDBItem(self, object):
        item = {
        'id': {'S': object['widgetId']},
        'owner': {'S': object['owner']},
        'label': {'S': object['label']},
        'description': {'S': object['description']},
        'otherAttributes': {'L': []}  # Initialize an empty list
    }

        if 'otherAttributes' in object:
            for attribute in object['otherAttributes']:
                if 'name' in attribute and 'value' in attribute:
                    item['otherAttributes']['L'].append({
                        'M': {
                            'name': {'S': attribute['name']},
                            'value': {'S': attribute['value']}
                        }
                    })
        return item
        # Your createDBItem implementation here

    def updateDBItem(self, object, dbname):
        # Your updateDBItem implementation here
        pass

    def deleteDBItem(self, object, dbname):
        # Your deleteDBItem implementation here
        pass

    def process_request_db(self, type, source, dbname=''):
        while True:
            if(type == "queue"):
                queue_name = source
                response =self.queue.get_queue_url(QueueName=queue_name)
                queue_url = response['QueueUrl']
                try:
                    response = self.queue.receive_message(
                        QueueUrl=queue_url,
                        MaxNumberOfMessages=1,
                        VisibilityTimeout=10,
                        WaitTimeSeconds=10
                    )
                except Exception as e:
                    logging.error(e)
                if 'Messages' in response:
                    object = response['Messages'][0]
                    data = object['Body']
                    json_data = json.loads(data)
                    if data:
                        try:
                            json_data = json.loads(data)
                            if(json_data['type'] == 'create'):
                                item = self.createDBItem(json_data)
                                response = self.dynamoDB.put_item(TableName=dbname,Item=item)
                                logging.info(f'Object uploaded successfully to {dbname}')
                        except Exception as e:
                            logging.error(e)
                        try:
                            response = self.queue.delete_message(QueueUrl=queue_url, ReceiptHandle=object['ReceiptHandle'])
                            logging.info(f"deleted{response}")
                        except Exception as e:
                            logging.error(f"Error deleting message: {e}")
                        
            else:
                response = self.s3.list_objects(Bucket=source)
                print(source)
                if 'Contents' in response:
                    # Get the object key and retrieve the object
                    object = response['Contents'][0]
                    print(object)
                    response = self.s3.get_object(Bucket=source, Key=object['Key'])

                    # Process the object as needed
                    data = response['Body'].read().decode('utf-8')
                    if data:
                        try:
                            json_data = json.loads(data)
                            if(json_data['type'] == 'create'):
                                item = self.createDBItem(json_data)
                                response = self.dynamoDB.put_item(TableName=dbname,Item=item)
                                logging.info(f'Object uploaded successfully to {dbname}')
                        except Exception as e:
                            logging.error(e)
                    try:
                        self.s3.delete_object(Bucket=source, Key=object['Key'])
                    except Exception as e:
                        logging.error(e)
                else:
                    time.sleep(10)
                    print("waiting")

    def process_requests_bucket(self,type,source, destination_bucket, prefix=""):
        condition = True
        while condition:
            if(type == "queue"):
                    queue_name = source
                    response =self.queue.get_queue_url(QueueName=queue_name)
                    queue_url = response['QueueUrl']
                    try:
                        response = self.queue.receive_message(
                            QueueUrl=queue_url,
                            MaxNumberOfMessages=1,
                            VisibilityTimeout=10,
                            WaitTimeSeconds=10
                        )
                    except Exception as e:
                        logging.error(e)
                    if 'Messages' in response:
                        object = response['Messages'][0]
                        data = object['Body']
                        json_data = json.loads(data)
                        if data:
                            try:
                                json_data = json.loads(data)
                                if(json_data['type'] == 'create'):
                                    self.s3.put_object(Bucket=destination_bucket, Key=json_data['requestId'], Body=data)
                                    logging.info('Object uploaded successfully to '+ json_data['requestId'])
                            except Exception as e:
                                logging.error(e)
                            try:
                                response = self.queue.delete_message(QueueUrl=queue_url, ReceiptHandle=object['ReceiptHandle'])
                                logging.info(f"deleted: {response}")
                            except Exception as e:
                                logging.error(f"Error deleting message: {e}")
            else:
                response = self.s3.list_objects(Bucket=source)
                if 'Contents' in response:
                    # Get the object key and retrieve the object
                    object = response['Contents'][0]
                    response = self.s3.get_object(Bucket=source, Key=object['Key'])

                    # Process the object as needed
                    data = response['Body'].read().decode('utf-8')
                    if data:
                        try:
                            json_data = json.loads(data)
                            if(json_data['type'] == 'create'):
                                self.s3.put_object(Bucket=destination_bucket, Key=object['Key'], Body=data)
                                logging.info('Object uploaded successfully to '+ object['Key'])
                        except Exception as e:
                            logging.error(e)
                    try:
                        self.s3.delete_object(Bucket=source, Key=object['Key'])
                    except Exception as e:
                        logging.error(e)
                else:
                    time.sleep(10)
            # Sleep for a period before checking again (adjust as needed)


            


    def main(self):
        parser = argparse.ArgumentParser(description="A simple command-line tool")
        subparsers = parser.add_subparsers(title='subcommands', dest='command')

        s3_parser = subparsers.add_parser('s3', help='Indicate bucket to bucket transfer')
        s3_parser.add_argument('source_bucket', help='Bucket to pull requests from')
        s3_parser.add_argument('destination_bucket', help='Destination S3 Bucket to send requests')
        s3_parser.add_argument('--prefix', help='possible folder', required=False)

        dynomoDB_parser = subparsers.add_parser('dynamodb', help='Move items from bucket to DynamoDB')
        dynomoDB_parser.add_argument('source_bucket', help='Bucket to pull requests from')
        dynomoDB_parser.add_argument('db_name', help='Name of DynamoDB table to add to')
        dynomoDB_parser.add_argument('--prefix', help='possible folder', required=False)
        
        
        queueParser = subparsers.add_parser('queue', help='Process items from a queue')
        queueParser.add_argument('queueName', help="name of the queue")
        queueParser.add_argument('--target', choices=['db', 's3'], help='Specify the target (db or s3) for queue processing')
        queueParser.add_argument('--destination', help='destination for requests')


        args = parser.parse_args()

        if args.command == 's3':
            self.process_requests_bucket('s3',args.source_bucket, args.destination_bucket, args.prefix)
        elif args.command == 'dynamodb':
            self.process_request_db('s3',args.source_bucket, args.db_name)
        elif args.command == 'queue':
            if args.target == 'db':
                self.process_request_db('queue', args.queueName, args.destination)
            elif args.target == 's3':
                self.process_requests_bucket('queue',args.queueName,args.destination)
            else:
                print("Invalid target specified. Use ['db' or 's3']")


if __name__ == '__main__':
    data_processor = Consumer()
    data_processor.main()
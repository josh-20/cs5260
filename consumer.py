import argparse
import boto3
import time
import logging
import json
import traceback

class Consumer:
    def __init__(self):
        logging.basicConfig(filename="consumer.log", level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.s3 = boto3.client('s3')
        self.dynamoDB = boto3.client('dynamodb', 'us-east-1')
        self.queue = boto3.client('sqs', 'us-east-1')

    def createDBItem(self, object):
        item = {
        'id': {'S': object['widgetId']},
        'requestId': {'S': object['requestId']},
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

    def updateDBItem(self, json_data, dbname):
        # Your updateDBItem implementation here
        try:
            key = {'id': {'S': json_data['widgetId']}}
            
            if('label' not in json_data):
                update_expression = 'SET requestId = :newRequestId, #owner = :newOwner, description = :newDescription, otherAttributes = :newOtherAttributes'
                expression_attribute_values = {
                    ':newRequestId': {'S': json_data['requestId']},
                    ':newOwner': {'S': json_data['owner']},
                    ':newDescription': {'S': json_data['description']},
                    ':newOtherAttributes': {'L': []}
                }
                expression_attribute_names = {
                    '#owner': 'owner',  # Define the placeholder for 'owner'
                }
                if 'otherAttributes' in json_data:
                    for attribute in json_data['otherAttributes']:
                        if 'name' in attribute and 'value' in attribute:
                            expression_attribute_values[':newOtherAttributes']['L'].append({
                                'M': {
                                    'name': {'S': attribute['name']},
                                    'value': {'S': attribute['value']}
                                }
                            })
                
                response = self.dynamoDB.update_item(TableName=dbname, Key=key, UpdateExpression=update_expression, ExpressionAttributeValues=expression_attribute_values,ExpressionAttributeNames=expression_attribute_names)
                logging.info(f"Update item in {dbname}{response}")
            else:
                update_expression = 'SET requestId = :newRequestId, #owner = :newOwner, #label = :newLabel, description = :newDescription, otherAttributes = :newOtherAttributes'
                expression_attribute_values = {
                    ':newRequestId': {'S': json_data['requestId']},
                    ':newOwner': {'S': json_data['owner']},
                    ':newLabel': {'S': json_data['label']},
                    ':newDescription': {'S': json_data['description']},
                    ':newOtherAttributes': {'L': []}
                }
                expression_attribute_names = {
                    '#owner': 'owner',  # Define the placeholder for 'owner'
                    '#label': 'label'  # Define the placeholder for 'label'
                }
                if 'otherAttributes' in json_data:
                    for attribute in json_data['otherAttributes']:
                        if 'name' in attribute and 'value' in attribute:
                            expression_attribute_values[':newOtherAttributes']['L'].append({
                                'M': {
                                    'name': {'S': attribute['name']},
                                    'value': {'S': attribute['value']}
                                }
                            })
                
                response = self.dynamoDB.update_item(TableName=dbname, Key=key, UpdateExpression=update_expression, ExpressionAttributeValues=expression_attribute_values,ExpressionAttributeNames=expression_attribute_names)
                logging.info(f"Update item in {dbname}{response}")
        except Exception as e:
            logging.error(f"Issue Updating dynamoDB Item {e}")
            traceback.print_exc()
 

    def process_DB_request(self, dbname, data):
        try:
            json_data = json.loads(data)
            if(json_data['type'] == 'create'):
                item = self.createDBItem(json_data)
                response = self.dynamoDB.put_item(TableName=dbname,Item=item)
                logging.info(f'Object uploaded successfully to {dbname}')
            elif(json_data['type'] == 'delete'):
                try:
                    key = {'id': {'S': json_data['widgetId']}}
                    response = self.dynamoDB.delete_item(TableName=dbname, Key=key)
                    logging.info(f"Deleted Item from {dbname}{response}")
                except Exception as e:
                    logging.error(f"Issues Deleting Item from dynamoDB {e}")
            elif(json_data['type'] == 'update'):
                self.updateDBItem(json_data,dbname)
                
        except Exception as e:
            logging.error(e)

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
                        self.process_DB_request(dbname,data)
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
                        self.process_DB_request(dbname,data)
                    try:
                        self.s3.delete_object(Bucket=source, Key=object['Key'])
                    except Exception as e:
                        logging.error(e)
                else:
                    time.sleep(10)
                    print("waiting")
                    
    def process_s3_Request(self,json_data, destination_bucket, data):
        try:
            if(json_data['type'] == 'create'):
                self.s3.put_object(Bucket=destination_bucket, Key=json_data['requestId'], Body=data)
                logging.info('Object uploaded successfully to '+ json_data['requestId'])
            elif (json_data['type'] == 'delete'):
                try:
                    response = self.s3.delete_object(Bucket=destination_bucket, Key=json_data['requestId'])
                    logging.info(f"Deleted: {response}")
                except Exception as e:
                    logging.error(f"Issue Deleting from S3 bucket: {e}")
            elif(json_data['type'] == 'update'):
                try:
                    response = self.s3.put_object(Bucket=destination_bucket, Key=json_data['requestId'], Body=data)
                    logging.info(f"updated Item: {response}")
                except Exception as e:
                    logging.error(f"Issue updating Item in s3 bucket: {e}")
        except Exception as e:
            logging.error(f"Issue in process_s3_Request: {e}")

    def process_requests_bucket(self,type,source, destination_bucket):
        condition = True
        while condition:
            if(type == "queue"):
                    response =self.queue.get_queue_url(QueueName=source)
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
                            self.process_s3_Request(json_data, destination_bucket, data)
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
                    json_data = json.loads(data)
                    if data:
                        self.process_s3_Request(json_data, destination_bucket, data)
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
            self.process_requests_bucket('s3',args.source_bucket, args.destination_bucket)
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
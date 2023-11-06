FROM python:3.9
COPY consumer.py consumer.py
RUN pip install boto3
CMD ["python", "consumer.py","s3","usu-cs5260-joshua-requests","usu-cs5260-joshua-web"]

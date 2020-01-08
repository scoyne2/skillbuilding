FROM python:3.7-slim-stretch

RUN pip install TwitterAPI
RUN pip install boto3

COPY producer/ /producer

CMD [ "python3", "producer/twitter-kinesis.py" ]
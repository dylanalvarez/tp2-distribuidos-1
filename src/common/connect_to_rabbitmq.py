from contextlib import contextmanager
from time import sleep

import pika


@contextmanager
def connect_to_rabbitmq():
    connection = None
    for _ in range(20):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        except pika.exceptions.AMQPConnectionError:
            sleep(1)
    if not connection:
        exit(1)
    try:
        yield connection.channel()
    finally:
        connection.close()

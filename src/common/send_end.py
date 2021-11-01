import time


def send_end(channel, exchange, routing_key):
    print(f'sending end to exchange "{exchange}", routing key "{routing_key}"')
    channel.basic_publish(exchange=exchange, routing_key=routing_key, body='__end__')

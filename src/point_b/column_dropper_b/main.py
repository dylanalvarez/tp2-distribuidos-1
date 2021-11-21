#!/usr/bin/env python3
import os
import json

from common.connect_to_rabbitmq import connect_to_rabbitmq
from common.send_end import send_end
from common.get_node_id import get_node_id

with connect_to_rabbitmq() as channel:
    accumulator_count = int(os.getenv('USER_SCORE_BUCKET_COUNT'))
    row_type = os.getenv('ROW_TYPE')
    channel.queue_declare(queue=f'point_b_{row_type}s')
    channel.queue_declare(queue=f'{row_type}_scores')
    for node_id in range(accumulator_count):
        channel.queue_declare(queue=f'{row_type}_user_with_score_{node_id}')

    def handle_row(_ch, method, _properties, body):
        body = body.decode("ISO-8859-1")
        if body == '__end__':
            send_end(channel, exchange='', routing_key=f'{row_type}_scores')
            for node_id in range(accumulator_count):
                send_end(channel, exchange='', routing_key=f'{row_type}_user_with_score_{node_id}')
            channel.basic_ack(delivery_tag=method.delivery_tag)
            exit(0)
        else:
            row = json.loads(body)
            channel.basic_publish(exchange='', routing_key=f'{row_type}_scores', body=row['score'])
            channel.basic_publish(
                exchange='',
                routing_key=f'{row_type}_user_with_score_{str(get_node_id(row["user_id"], accumulator_count))}',
                body=' '.join((row['user_id'], str(row['score'])))
            )

    channel.basic_consume(queue=f'point_b_{row_type}s', on_message_callback=handle_row)
    channel.start_consuming()

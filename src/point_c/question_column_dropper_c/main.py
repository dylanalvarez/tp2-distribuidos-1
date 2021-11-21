#!/usr/bin/env python3
import os
import json

from common.connect_to_rabbitmq import connect_to_rabbitmq
from common.send_end import send_end
from common.get_node_id import get_node_id

with connect_to_rabbitmq() as channel:
    joiner_by_question_id_count = int(os.getenv('JOINER_BY_QUESTION_ID_COUNT'))
    channel.queue_declare(queue='point_c_questions')
    for node_id in range(joiner_by_question_id_count):
        channel.queue_declare(queue=f'question_id_with_year_score_{node_id}')

    def handle_question(_ch, method, _properties, body):
        body = body.decode("ISO-8859-1")
        if body == '__end__':
            for node_id in range(joiner_by_question_id_count):
                send_end(channel, exchange='', routing_key=f'question_id_with_year_score_{node_id}')
            channel.basic_ack(delivery_tag=method.delivery_tag)
            exit(0)
        else:
            question = json.loads(body)
            channel.basic_publish(
                exchange='',
                routing_key=f'question_id_with_year_score_{str(get_node_id(question["question_id"], joiner_by_question_id_count))}',
                body=' '.join((question["question_id"], question["creation_date"][0:4], question["score"], question["tags"]))
            )

    channel.basic_consume(queue='point_c_questions', on_message_callback=handle_question)
    channel.start_consuming()

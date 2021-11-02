#!/usr/bin/env python3
import csv
import os

from common.connect_to_rabbitmq import connect_to_rabbitmq
from common.get_node_id import get_node_id
from common.send_end import send_end

with connect_to_rabbitmq() as channel:
    accumulator_count = int(os.getenv('USER_SCORE_BUCKET_COUNT'))
    joiner_by_question_id_count = int(os.getenv('JOINER_BY_QUESTION_ID_COUNT'))

    channel.queue_declare(queue='question_scores')
    for node_id in range(accumulator_count):
        channel.queue_declare(queue=f'question_user_with_score_{node_id}')
    for node_id in range(joiner_by_question_id_count):
        channel.queue_declare(queue=f'question_id_with_year_score_{node_id}')

    with open('/data/questions.csv', encoding="ISO-8859-1") as file:
        questions = csv.reader(file)
        _header = next(questions)
        for question in questions:
            _, id, user_id, creation_date, closed_date, score, title, body, tags = question
            channel.basic_publish(exchange='', routing_key='question_scores', body=score)
            channel.basic_publish(exchange='', routing_key=f'question_user_with_score_{str(get_node_id(user_id, accumulator_count))}', body=' '.join((user_id, str(score))))
            channel.basic_publish(exchange='', routing_key=f'question_id_with_year_score_{str(get_node_id(id, joiner_by_question_id_count))}', body=' '.join((id, creation_date[0:4], score, tags)))

    send_end(channel, exchange='', routing_key='question_scores')
    for node_id in range(accumulator_count):
        send_end(channel, exchange='', routing_key=f'question_user_with_score_{node_id}')
    for node_id in range(joiner_by_question_id_count):
        send_end(channel, exchange='', routing_key=f'question_id_with_year_score_{node_id}')
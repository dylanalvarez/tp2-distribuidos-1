#!/usr/bin/env python3
import csv
import os

from common.connect_to_rabbitmq import connect_to_rabbitmq
from common.get_node_id import get_node_id
from common.send_end import send_end

with connect_to_rabbitmq() as channel:
    sentiment_calculator_count = int(os.getenv('SENTIMENT_CALCULATOR_COUNT'))
    accumulator_count = int(os.getenv('USER_SCORE_BUCKET_COUNT'))
    joiner_by_question_id_count = int(os.getenv('JOINER_BY_QUESTION_ID_COUNT'))

    channel.queue_declare(queue='answer_bodies')
    channel.queue_declare(queue='answer_scores')
    for node_id in range(accumulator_count):
        channel.queue_declare(queue=f'answer_user_with_score_{node_id}')
    for node_id in range(joiner_by_question_id_count):
        channel.queue_declare(queue=f'answer_id_with_year_score_{node_id}')

    with open('/data/answers.csv', encoding="ISO-8859-1") as file:
        answers = csv.reader(file)
        _header = next(answers)
        for answer in answers:
            _, answer_id, user_id, creation_date, question_id, score, body = answer
            channel.basic_publish(exchange='', routing_key='answer_scores', body=score)
            channel.basic_publish(exchange='', routing_key=f'answer_user_with_score_{str(get_node_id(user_id, accumulator_count))}', body=' '.join((user_id, str(score))))
            channel.basic_publish(exchange='', routing_key=f'answer_id_with_year_score_{str(get_node_id(question_id, joiner_by_question_id_count))}', body=' '.join((question_id, creation_date[0:4], score)))
            if float(score) > 10:
                channel.basic_publish(exchange='', routing_key='answer_bodies', body=body)

    send_end(channel, exchange='', routing_key='answer_scores')
    for _ in range(sentiment_calculator_count):
        send_end(channel, exchange='', routing_key='answer_bodies')
    for node_id in range(accumulator_count):
        send_end(channel, exchange='', routing_key=f'answer_user_with_score_{node_id}')
    for node_id in range(joiner_by_question_id_count):
        send_end(channel, exchange='', routing_key=f'answer_id_with_year_score_{node_id}')

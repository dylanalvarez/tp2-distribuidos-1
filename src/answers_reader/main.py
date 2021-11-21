#!/usr/bin/env python3
import csv
import json

from common.connect_to_rabbitmq import connect_to_rabbitmq
from common.send_end import send_end

with connect_to_rabbitmq() as channel:
    channel.queue_declare(queue='point_a_answers')
    channel.queue_declare(queue='point_b_answers')
    channel.queue_declare(queue='point_c_answers')

    with open('/data/answers.csv', encoding="ISO-8859-1") as file:
        answers = csv.reader(file)
        _header = next(answers)
        for answer in answers:
            _, answer_id, user_id, creation_date, question_id, score, body = answer
            answer_json = json.dumps({
                'answer_id': answer_id,
                'user_id': user_id,
                'creation_date': creation_date,
                'question_id': question_id,
                'score': score,
                'body': body,
            })
            channel.basic_publish(exchange='', routing_key='point_a_answers', body=answer_json)
            channel.basic_publish(exchange='', routing_key='point_b_answers', body=answer_json)
            channel.basic_publish(exchange='', routing_key='point_c_answers', body=answer_json)

    send_end(channel, exchange='', routing_key='point_a_answers')
    send_end(channel, exchange='', routing_key='point_b_answers')
    send_end(channel, exchange='', routing_key='point_c_answers')

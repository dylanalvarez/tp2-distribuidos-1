#!/usr/bin/env python3
import math
import os

from common.connect_to_rabbitmq import connect_to_rabbitmq
from common.send_end import send_end

with connect_to_rabbitmq() as channel:
    node_id = os.getenv('NODE_ID')
    joiner_by_question_id_count = int(os.getenv('JOINER_BY_QUESTION_ID_COUNT'))
    channel.queue_declare(queue=f'year_score_sum_tags_{node_id}')
    channel.queue_declare(queue=f'year_and_top_tags')

    score_by_tag_by_year = {}
    received_end_count = 0

    def build_top_10_for_year(year, score_by_tag):
        top_10 = []
        for tag, score in score_by_tag.items():
            if len(top_10) < 10:
                top_10.append((tag, score))
            elif score > top_10[-1][1]:
                top_10[-1] = (tag, score)
            top_10.sort(key=lambda x: x[1], reverse=True)
        return ' '.join((year, str(top_10)))


    def handle_year_score_sum_tags(_ch, method, _properties, body):
        global received_end_count
        body = body.decode("ISO-8859-1")
        if body == '__end__':
            received_end_count += 1
            if received_end_count == joiner_by_question_id_count:
                for year, score_by_tag in score_by_tag_by_year.items():
                    channel.basic_publish(exchange='', routing_key='year_and_top_tags', body=build_top_10_for_year(year, score_by_tag))
                send_end(channel, exchange='', routing_key='year_and_top_tags')
                channel.basic_ack(delivery_tag=method.delivery_tag)
                exit(0)
            else:
                channel.basic_ack(delivery_tag=method.delivery_tag)
        else:
            year, score, *tags = body.split(' ')
            score = float(score)
            score = 0 if math.isnan(score) else score
            for tag in tags:
                if year not in score_by_tag_by_year:
                    score_by_tag_by_year[year] = {}
                if tag not in score_by_tag_by_year[year]:
                    score_by_tag_by_year[year][tag] = 0
                score_by_tag_by_year[year][tag] += score
            channel.basic_ack(delivery_tag=method.delivery_tag)


    channel.basic_consume(queue=f'year_score_sum_tags_{node_id}', on_message_callback=handle_year_score_sum_tags)

    channel.start_consuming()

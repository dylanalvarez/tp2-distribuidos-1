#!/usr/bin/env python3
import math
import os

from common.connect_to_rabbitmq import connect_to_rabbitmq

with connect_to_rabbitmq() as channel:
    joiner_by_year_count = int(os.getenv('JOINER_BY_YEAR_COUNT'))
    channel.queue_declare(queue='year_and_top_tags')

    received_end_count = 0
    top_by_year = []

    def handle_year_and_top_tags(_ch, method, _properties, body):
        global received_end_count
        body = body.decode("ISO-8859-1")
        if body == '__end__':
            received_end_count += 1
            if received_end_count == joiner_by_year_count:
                top_by_year.sort(key=lambda x: x[0])
                with open('/results/top_10_tags_by_year.txt', 'w+') as file:
                    for year, top_10 in top_by_year:
                        file.write(f'{year} {top_10}\n')
                channel.basic_ack(delivery_tag=method.delivery_tag)
                exit(0)
            else:
                channel.basic_ack(delivery_tag=method.delivery_tag)
        else:
            year, top_10 = body.split(' ', 1)
            top_by_year.append((year, top_10))


    channel.basic_consume(queue='year_and_top_tags', on_message_callback=handle_year_and_top_tags)

    channel.start_consuming()

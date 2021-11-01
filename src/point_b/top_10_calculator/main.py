#!/usr/bin/env python3
import os

from common.connect_to_rabbitmq import connect_to_rabbitmq

with connect_to_rabbitmq() as channel:
    channel.queue_declare(queue='user_with_joined_score')

    accumulator_count = int(os.getenv('USER_SCORE_BUCKET_COUNT'))

    top_users = []
    finished_accumulators = 0

    def handle_user_with_joined_score(_ch, method, _properties, body):
        body = body.decode("ISO-8859-1")
        print(f'received {body}')
        global accumulator_count
        global top_users
        global finished_accumulators
        if body == '__end__':
            print('user_with_joined_score ended')
            finished_accumulators += 1
            if finished_accumulators == accumulator_count:
                with open('/results/top_10_calculator.txt', 'w+') as file:
                    if len(top_users) == 0:
                        file.write('Ended with no users')
                    else:
                        file.write(str(top_users))
                channel.basic_ack(delivery_tag=method.delivery_tag)
                exit(0)
        else:
            user_id, score = body.split(' ')
            if user_id:
                if len(top_users) < 10:
                    top_users.append((user_id, score))
                elif score > top_users[-1][1]:
                    top_users[-1] = (user_id, score)
                top_users.sort(key=lambda x: x[1], reverse=True)
            channel.basic_ack(delivery_tag=method.delivery_tag)


    channel.basic_consume(queue='user_with_joined_score', on_message_callback=handle_user_with_joined_score)
    channel.start_consuming()


# TODO: top_10_calculator varía!

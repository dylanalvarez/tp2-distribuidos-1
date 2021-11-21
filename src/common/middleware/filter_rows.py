import json

from common.connect_to_rabbitmq import connect_to_rabbitmq
from common.send_end import send_end


class FilterRows:
    def __init__(self, input_queue, output_queue, output_end_message_count, should_discard_row, map_row_to_message_string):
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.output_end_message_count = output_end_message_count
        self.should_discard_row = should_discard_row
        self.map_row_to_message_string = map_row_to_message_string

    def run(self):
        with connect_to_rabbitmq() as channel:
            channel.queue_declare(queue=self.input_queue)
            channel.queue_declare(queue=self.output_queue)

            def handle_row(_ch, method, _properties, body):
                body = body.decode("ISO-8859-1")
                if body == '__end__':
                    for _ in range(self.output_end_message_count):
                        send_end(channel, exchange='', routing_key=self.output_queue)
                    channel.basic_ack(delivery_tag=method.delivery_tag)
                    exit(0)
                else:
                    row = json.loads(body)
                    if not self.should_discard_row(row):
                        channel.basic_publish(
                            exchange='',
                            routing_key=self.output_queue,
                            body=self.map_row_to_message_string(row)
                        )

            channel.basic_consume(queue=self.input_queue, on_message_callback=handle_row)
            channel.start_consuming()

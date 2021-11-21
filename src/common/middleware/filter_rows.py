import json

from common.connect_to_rabbitmq import connect_to_rabbitmq
from common.send_end import send_end


class FilterRows:
    def __init__(self, input_queue, map_row_to_message_by_output_queue, end_message_count_by_output_queue, should_discard_row=lambda _: False):
        self.input_queue = input_queue
        self.map_row_to_message_by_output_queue = map_row_to_message_by_output_queue
        self.end_message_count_by_output_queue = end_message_count_by_output_queue
        self.should_discard_row = should_discard_row

    def run(self):
        with connect_to_rabbitmq() as channel:
            channel.queue_declare(queue=self.input_queue)
            for output_queue in self.end_message_count_by_output_queue.keys():
                channel.queue_declare(queue=output_queue)

            def handle_row(_ch, method, _properties, body):
                body = body.decode("ISO-8859-1")
                if body == '__end__':
                    for output_queue, end_message_count in self.end_message_count_by_output_queue.items():
                        for _ in range(end_message_count):
                            send_end(channel, exchange='', routing_key=output_queue)
                    channel.basic_ack(delivery_tag=method.delivery_tag)
                    exit(0)
                else:
                    row = json.loads(body)
                    if not self.should_discard_row(row):
                        for output_queue, message_string in self.map_row_to_message_by_output_queue(row).items():
                            channel.basic_publish(
                                exchange='',
                                routing_key=output_queue,
                                body=message_string
                            )

            channel.basic_consume(queue=self.input_queue, on_message_callback=handle_row)
            channel.start_consuming()

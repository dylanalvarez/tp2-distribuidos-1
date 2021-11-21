import json


class FilterRows:
    def __init__(
            self,
            input_queue,
            map_row_to_message_by_output_queue,
            end_message_count_by_output_queue,
            should_discard_row=lambda _: False
    ):
        self.input_queue = input_queue
        self.map_row_to_message_by_output_queue = map_row_to_message_by_output_queue
        self.end_message_count_by_output_queue = end_message_count_by_output_queue
        self.should_discard_row = should_discard_row

    def run(self):
        def handle_row(body, ack):
            row = json.loads(body)
            if not self.should_discard_row(row):
                for output_queue, message_string in self.map_row_to_message_by_output_queue(row).items():
                    output_queue.send_message(message_string)
            ack()

        def handle_end(ack):
            print('entre al handler de end')
            for output_queue, end_message_count in self.end_message_count_by_output_queue.items():
                for _ in range(end_message_count):
                    output_queue.send_end()
            ack()
            exit(0)

        self.input_queue.listen(on_end=handle_end, on_message=handle_row)

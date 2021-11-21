END = '__end__'


class Queue:
    def __init__(self, name, rabbitmq_channel):
        rabbitmq_channel.queue_declare(queue=name)
        self.name = name
        self.rabbitmq_channel = rabbitmq_channel

    def send_message(self, message):
        self.rabbitmq_channel.basic_publish(exchange='', routing_key=self.name, body=message)

    def send_end(self):
        self.rabbitmq_channel.basic_publish(exchange='', routing_key=self.name, body=END)

    def listen(self, on_end, on_message):
        self.rabbitmq_channel.basic_consume(
            queue=self.name,
            on_message_callback=self._build_on_message_callback(on_end, on_message)
        )
        self.rabbitmq_channel.start_consuming()

    def _build_on_message_callback(self, on_end, on_message):
        def on_message_callback(_ch, method, _properties, body):
            body = body.decode("ISO-8859-1")

            def ack():
                self.rabbitmq_channel.basic_ack(delivery_tag=method.delivery_tag)
            if body == END:
                on_end(ack=ack)
            else:
                on_message(body=body, ack=ack)

        return on_message_callback

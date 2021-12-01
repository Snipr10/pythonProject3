import pika


def main():
    parameters = pika.URLParameters("amqp://full_posts_parser:nJ6A07XT5PgY@192.168.5.46:5672/smi_tasks")
    connection = pika.BlockingConnection(parameters=parameters)
    channel = connection.channel()

    def callback(ch, method, properties, body):
        print(" [x] Received %r" % body)
        # ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue='full_posts_tasks', on_message_callback=callback)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')

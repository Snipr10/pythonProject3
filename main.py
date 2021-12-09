import json

import pika


def main():
    parameters = pika.URLParameters("amqp://tester:trotilla@192.168.5.46:5672/test")
    connection = pika.BlockingConnection(parameters=parameters)
    channel = connection.channel(channel_number=123)

    rmq_json_data = {
        "title": "Стало известно, кем этим летом смогут подработать петербургские студенты",
        "content": "Петербургские студенты летом начинают традиционный поиск подработки. Аналитики выяснили, на что сейчас могут рассчитывать начинающие специалисты.<br> При этом сами соискатели тоже пока не проявляют большой активности в поиске работы: количество резюме от начинающих специалистов в апреле этого года сократилось на 3% по сравнению с прошлым годом.<br> По мнению экспертов, работу студентам лучше искать в сфере продаж (41% от всех вакансий в этом разделе), консультирования (12%), ИТ и интернета (12%), административного персонала (10%), потенциальных работников гостинично-ресторанного бизнеса (10%), транспорта и логистики (9%), а также маркетинга, рекламы и PR (6%). Кроме того, работодатели ищут менеджеров по продажам и по работе с клиентами, операторов call-центров, уборщиков, курьеров, поваров, ассистентов, консультантов. Средняя зарплата для начинающих специалистов и студентов превысила уровень зарплатных ожиданий соискателей – 37,5 тыс. рублей в вакансиях против 35 тыс. рублей в резюме.<br> Напомним, ранее «Мойка78» писала, что названы сферы, в которых вырастет число вакансий на «удаленке».<br>https://moika78.ru/news2/2020/05/student-849822_1920-1024x683.jpg",
        "created": "2020-05-26 11:03:00",
        "url": "https://moika78.ru/news/2020-05-26/420771-stalo-izvestno-kem-etim-letom-smogut-podrabotat-peterburgskie-studenty/",
        "author_name": "Мойка78",
        "author_icon": "//avatars.mds.yandex.net/get-ynews-logo/62808/254155375-1518429252503-square/logo-square",
        "group_id": "",
        "images": [],
        "keyword_id": 10000002,
    }
    while True:
        try:
            channel.basic_publish(exchange='',
                                  routing_key='test',
                                  body=json.dumps(rmq_json_data))
            print("SAVE")
        except Exception as e:
            print(e)
    connection.close()

    parameters = pika.URLParameters("amqp://tester:trotilla@192.168.5.46:5672/test")
    connection = pika.BlockingConnection(parameters=parameters)
    channel = connection.channel(channel_number=124)
    channel.queue_declare("test")
    def callback(ch, method, properties, body):
        print(ch.channel_number)
        print(" [x] Received %r" % body)
        # ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue='test', on_message_callback=callback, auto_ack=False)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')

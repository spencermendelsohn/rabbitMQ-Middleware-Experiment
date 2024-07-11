#!/usr/bin/env python
import json
import time
import pika, sys, os


def main():
    article_views = {}
    user_views = {}
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='hello')

    global counters

    counters = 0


    def callback(ch, method, properties, body):
        global counters
        temp = json.loads(body.decode('utf-8'))
        # Article views counter
        if article_views.get(temp['article']['name']) is not None:
            article_views[temp['article']['name']] += 1
        else:
            article_views[temp['article']['name']] = 1

        # User views counter
        if user_views.get(temp['viewer']['id']) is not None:
            user_views[temp['viewer']['id']] += 1
        else:
            user_views[temp['viewer']['id']] = 1

        if counters % 100 == 0:
            print(f" [x] Received {temp}")
            print(f" [x] User Views: {user_views}")
            print(f" [x] Article Views: {article_views}")
            latency = time.time() - temp['time_stamp']
            print(f" [x] Latency: {latency*1000}ms")
            print(f" [x] Counters: {counters}")
        counters += 1
    channel.basic_consume(queue='article_views', on_message_callback=callback, auto_ack=True)
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
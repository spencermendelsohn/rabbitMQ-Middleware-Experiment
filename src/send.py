#!/usr/bin/env python
import pika
import json
import random
import time

# Random article titles generated from ABC.net.au
article_titles = [
    "Exploring Australia's Unique Wildlife: From Kangaroos to Koalas",
    "The Great Barrier Reef: Australia's Underwater Wonderland",
    "Aboriginal Culture and Art: A Journey into Australia's Indigenous Heritage",
    "Sydney vs. Melbourne: Contrasting Australia's Largest Cities",
    "Australian Cuisine: From Meat Pies to Vegemite",
    "The Outback: Australia's Remote and Enigmatic Heartland",
    "Surfing in Australia: Riding the Waves Down Under",
    "Australia's National Parks: Breathtaking Landscapes and Conservation Efforts",
    "Australian History: From Convict Colony to Modern Nation",
    "Australian Icons: From the Sydney Opera House to Uluru"
]
# Random names generated from ABC.net.au
users = [
    "Emily Kangaroo",
    "Coral Diver",
    "Elder Wongari",
    "Jamie Urbanite",
    "Sheila Outback",
    "Surfer Dave",
    "Ranger Alice Wilderness",
    "Historian Tom Convict",
    "Sydney Stone",
    "Ayers Rock"
]
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
queue = 'article_views'
channel.queue_declare(queue=queue)

def create_view():
    """
    Builder class for creating an arbitrary view
    :return: json
    """
    article_title = random.choice(article_titles)
    user_title = random.choice(users)
    return {
        "article": {
            "name": article_title,
        },
        "viewer": {
            "id": user_title
        },
        "time_stamp": time.time()
    }


def send_per_second(amount):
    """
    Function to send a number of messages every second.
    :param amount: int
    :return: None
    """
    sleep_time = 1 / amount
    messages_sent = 1
    iterations = 10
    count = 0
    while count < iterations*amount:
        data = create_view()
        channel.basic_publish(exchange='', routing_key=queue, body=json.dumps(data))
        time.sleep(1/amount)
        start_time = time.time()
        print(f"Messages sent: {messages_sent}")
        messages_sent += 1
        elapsed_time = time.time() - start_time
        if elapsed_time < sleep_time:
            time.sleep(sleep_time - elapsed_time)
        count += 1


send_per_second(100)
#!/usr/bin/env python

import json
import random

from kafka import KafkaProducer
from card_generator import generate_card
from geopoint import create_geopoint

CITIES = [
    {"lat": 30.2672, "lon": -97.7430608, "city": "austin"},
    {"lat": 34.0522, "lon": -118.2437, "city": "los angeles"},
    {"lat": 39.7392, "lon": -104.9903, "city": "denver"},
    {"lat": 32.7767, "lon": -96.7970, "city": "dallas"}
]

TOPIC = 'authorizations'
BOOTSTRAP_SERVERS = 'csa-test-1.vpc.cloudera.com:9092'


def get_latlon():
    geo = random.choice(CITIES)
    return create_geopoint(geo['lat'], geo['lon'])


def purchase():
    """Return a random amount in cents """
    return random.randrange(1000, 90000)


def get_user():
    """ return a random user """
    return random.randrange(0, 999)


def make_fraud(seed, card, user, latlon):
    """ return a fraudulent transaction """
    amount = (seed + 1) * 1000
    payload = {"userid": user,
               "amount": amount,
               "lat": latlon[0],
               "lon": latlon[1],
               "card": card
               }
    return payload


def fraud_loop(producer):
    payload = {}
    fraud_trigger = 15
    i = 1

    while True:
        payload = None
        if i % fraud_trigger == 0:
            # fraud
            print("fraud..")
            card = generate_card("visa16")
            user = get_user()
            latlon = get_latlon()
            for r in range(3):
                payload = make_fraud(r, card, user, latlon)
                print(f'fraud: {payload}')
                try:
                    producer.send(topic=TOPIC, value=payload)
                except Exception as ex:
                    print("unable to produce {}".format(ex))
        else:
            # not fraud
            latlon = get_latlon()
            payload = {
                "userid": get_user(),
                "amount": purchase(),
                "lat": latlon[0],
                "lon": latlon[1],
                "card": generate_card("visa16")
            }
        print(payload)
        try:
            producer.send(topic=TOPIC, value=payload)
            i += 1

        except Exception as ex:
            print("unable to produce {}".format(ex))



if __name__ == "__main__":
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        fraud_loop(producer)
    except Exception as e:
        print(e)

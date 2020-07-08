import argparse
import json
import logging
import os
import random
import time
import uuid
import datetime

from kafka import KafkaProducer

FAULT_CODES = [

        138,
        287,
        217,
        444,
        911,
]

EQUIP_IDS = [
    81,
    82,
    83, 
]

def generate_event():
    ret = {
        "faultCode": {
        'id': str(random.randint(100000000, 9999999999)),
        'faultId': FAULT_CODES[random.randint(0, 5)],
        'ecuId': FAULT_CODES[random.randint(0, 3)],
        'timestamp': "2020-06-22T17:26:14.000Z",
        }
    }
    return ret


def main(args):
    logging.info('brokers={}'.format(args.brokers))
    logging.info('topic={}'.format(args.topic))
    logging.info('rate={}'.format(args.rate))

    logging.info('creating kafka producer')
    producer = KafkaProducer(bootstrap_servers=args.brokers)

    logging.info('begin sending events')
    while True:
        producer.send(args.topic, json.dumps(generate_event()).encode())
        logging.info(generate_event())
        time.sleep(30.0)
    logging.info('end sending events')


def get_arg(env, default):
    return os.getenv(env) if os.getenv(env, '') is not '' else default


def parse_args(parser):
    args = parser.parse_args()
    args.brokers = get_arg('KAFKA_BROKERS', args.brokers)
    args.topic = get_arg('KAFKA_TOPIC', args.topic)
    args.rate = get_arg('RATE', args.rate)
    return args


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logging.info('starting kafka-openshift-python emitter')
    parser = argparse.ArgumentParser(description='emit some stuff on kafka')
    parser.add_argument(
            '--brokers',
            help='The bootstrap servers, env variable KAFKA_BROKERS',
            default='kafka:9092')
    parser.add_argument(
            '--topic',
            help='Topic to publish to, env variable KAFKA_TOPIC',
            default='events')
    parser.add_argument(
            '--rate',
            type=int,
            help='Lines per second, env variable RATE',
            default=3)
    args = parse_args(parser)
    main(args)
    logging.info('exiting')

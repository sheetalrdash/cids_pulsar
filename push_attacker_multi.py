import pulsar
import logging
import sys

ATTACKERS_TOPIC = 'persistent://sample/standalone/ns1/attacker'

# Setup for basic logging
logging.basicConfig(format='%(asctime)s %(levelname)s : %(message)s', level=logging.DEBUG)

def main(args):
    logging.info('Connecting to Pulsar...')

    # Create a pulsar client instance with reference to the broker
    client = pulsar.Client('pulsar://localhost:6650')

    # Build a producer instance on a specific topic
    producer = client.create_producer(ATTACKERS_TOPIC)
    logging.info('Connected to Pulsar')

    msg = '10.0.0.1'
    logging.info('Sending ACL Addition Request: %s ', msg)
    producer.send(msg)

    client.close()


if __name__ == '__main__':
    main(sys.argv)

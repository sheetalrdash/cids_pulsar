import pulsar
import logging
import sys

def pulsar_publish(attacker_ip):
	ATTACKERS_TOPIC = 'persistent://sample/standalone/ns1/attacker'

	# Setup for basic logging
	logging.basicConfig(format='%(asctime)s %(levelname)s : %(message)s', level=logging.DEBUG)
	logging.info('Connecting to Pulsar...')

	# Create a pulsar client instance with reference to the broker
	client = pulsar.Client('pulsar://localhost:6650')

	# Build a producer instance on a specific topic
	producer = client.create_producer(ATTACKERS_TOPIC)
	logging.info('Connected to Pulsar')

	logging.info('Sending ACL Addition Request: %s ', attacker_ip)
	producer.send(attacker_ip)

	client.close()
def main(args):
	pulsar_publish('10.0.0.1')

if __name__ == '__main__':
    main(sys.argv)
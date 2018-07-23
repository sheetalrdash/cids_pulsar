import pulsar
import logging
import sys
import os

ATTACKERS_TOPIC = 'persistent://sample/standalone/ns1/attacker'
SUBSCRIPTION = 'my-sub'
TIMEOUT = 10000

# Setup up basic logging
logging.basicConfig(format='%(asctime)s %(levelname)s : %(message)s', level=logging.DEBUG)

def main(args):
    logging.info('Connecting to Pulsar...')

    # Create a pulsar client instance with reference to the broker
    client = pulsar.Client('pulsar://localhost:6650')

    consumer = client.subscribe(ATTACKERS_TOPIC, SUBSCRIPTION)
    logging.info('Created consumer for the topic %s', ATTACKERS_TOPIC)

    while True:
        try:
            # try and receive messages with a timeout of 10 seconds
            msg = consumer.receive(timeout_millis=TIMEOUT)
	    logging.info("Received message '%s'", msg.data())
	    print "Test"
	    one = 'curl -X POST -d \''
	    print one
	    seven = "\' http://localhost:8080/wm/acl/rules/json"
	    print seven
	    command=one+msg.data()+seven
 	    #command='curl -X POST -d \'{"src-ip":"10.0.0.1/32","dst-ip":"10.0.0.2/32","action":"deny"}\' localhost:8080/wm/acl/rules/json'
	    #command=msg.data()
	    print command
	    logging.info("Post Command: "+command)
	    os.system(command)
            logging.info("Post Complete")
	    consumer.acknowledge(msg)  # send ack to pulsar for message consumption
        except Exception:
	    received = 0

    client.close()

if __name__ == '__main__':
    main(sys.argv)

import pulsar
import logging
import sys
import os, re, time

ATTACKERS_TOPIC = 'persistent://sample/standalone/ns1/attacker'
SUBSCRIPTION1 = 'sub1'
SUBSCRIPTION2 = 'sub2'
TIMEOUT = 10000

# Setup up basic logging
logging.basicConfig(format='%(asctime)s %(levelname)s : %(message)s', level=logging.DEBUG)

def main(args):
    logging.info('Connecting to Pulsar...')

    # Create a pulsar client instance with reference to the broker
    client = pulsar.Client('pulsar://localhost:6650')

    consumer1 = client.subscribe(ATTACKERS_TOPIC, SUBSCRIPTION1)
    consumer2 = client.subscribe(ATTACKERS_TOPIC, SUBSCRIPTION2)
    logging.info('Created consumer for the topic %s', ATTACKERS_TOPIC)

    while True:
        try:
            # try and receive messages with a timeout of 10 seconds
            msg = consumer1.receive(timeout_millis=TIMEOUT)
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
	    consumer1.acknowledge(msg)  # send ack to pulsar for message consumption
	    print "Sleeping"
	    time.sleep(60)
            # try and receive messages with a timeout of 10 seconds
            msg = consumer2.receive(timeout_millis=TIMEOUT)
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
	    consumer2.acknowledge(msg)  # send ack to pulsar for message consumption
        except Exception:
	    received = 0
    client.close()

if __name__ == '__main__':
    main(sys.argv)

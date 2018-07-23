import requests
import logging
import sys
import os, pulsar

ATTACKERS_TOPIC = 'persistent://sample/standalone/ns1/attacker'
SUBSCRIPTION = 'sub1'
TIMEOUT = 10000
#command='curl -X POST -d \'{"src-ip":"10.0.0.1/32","dst-ip":"10.0.0.2/32","action":"deny"}\' localhost:18080/wm/acl/rules/json'
# Setup up basic logging
logging.basicConfig(format='%(asctime)s %(levelname)s : %(message)s', level=logging.DEBUG)
def block_ip(attacker_ip):
    cntrlr_summ = requests.get("http://127.0.0.1:18080/wm/core/controller/summary/json")
    cntrlr_summ_json = cntrlr_summ.json()
    number_of_hosts = cntrlr_summ_json["# hosts"]
    device_detail = requests.get("http://127.0.0.1:18080/wm/device/")
    device_detail_json = device_detail.json()
    one = 'curl -X POST -d \'{"src-ip":"'+attacker_ip+'/32","dst-ip":"'
    three = '/32","action":"deny"}\' localhost:18080/wm/acl/rules/json'

    for index in range(0, number_of_hosts):
        device_ipv4 = str(device_detail_json["devices"][index]['ipv4'])
        if device_ipv4 != "[]":
            device_ipv4=device_ipv4.replace('[','').replace(']','').replace('u','')
            command= one + device_ipv4 + three
            print command
            os.system(command)

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
            logging.info("Post Complete")
    	    consumer.acknowledge(msg)  # send ack to pulsar for message consumption
        except Exception:
	        received = 0

if __name__ == '__main__':
    main(sys.argv)

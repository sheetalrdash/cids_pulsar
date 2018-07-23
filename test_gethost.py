import requests
import sys, logging, pulsar

def find_host( DPID , PORT):
    cntrlr_summ = requests.get("http://127.0.0.1:8080/wm/core/controller/summary/json")
    cntrlr_summ_json = cntrlr_summ.json()
    number_of_hosts = cntrlr_summ_json["# hosts"]
    device_detail = requests.get("http://127.0.0.1:8080/wm/device/")
    device_detail_json = device_detail.json()
    dpid_curr="1"
    port_curr="0"
    device_ipv4="1"
    for index in range(0, number_of_hosts-1):
        try:
            if(str(device_detail_json["devices"][index][u'attachmentPoint'])!='[]'):
                dpid_curr=str(device_detail_json["devices"][index]['attachmentPoint'][0]['switch'])
                port_curr=str(device_detail_json["devices"][index]['attachmentPoint'][0]['port'])
            if(dpid_curr==DPID and port_curr == PORT):
                device_ipv4 = str(device_detail_json["devices"][index]['ipv4']).replace('[','').replace(']','').replace('u','')
        except:
            device_ipv4=1
    print device_ipv4
    pulsar_publish(device_ipv4)

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
    find_host("00:00:00:00:00:00:00:02","5")

if __name__ == '__main__':
    main(sys.argv)
import logging
import sys
import os, pulsar, mysql.connector

SNIFFER_TOPIC = 'persistent://sample/standalone/ns1/ip_sniffer'
SUBSCRIPTION = 'sub1'
TIMEOUT = 10000
counter=0
source_ips=[]
dest_ips=[]
traffic=[]

# Setup up basic logging
logging.basicConfig(format='%(asctime)s %(levelname)s : %(message)s', level=logging.DEBUG)


def insert_db(traffic):
    mydb=mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="mysql",
        database="cids"
    )
    mycursor = mydb.cursor()

    sql = "INSERT INTO ip_sniffer (source_ip, dest_ip) VALUES (%s, %s)"
    mycursor.executemany(sql, traffic)

    mydb.commit()

    print(mycursor.rowcount, "record inserted.")

def traffic_listener(consumer, msg):
    global source_ips, dest_ips, traffic, counter
    #print "counter :"+counter
    while True:
	    logging.info("Received message '%s'", msg.data())
	    source_ips.append(msg.data().split("|")[0])
	    dest_ips.append(msg.data().split("|")[1])
	    print source_ips
	    counter=counter+1
	    if counter >= 5:
		traffic=list(zip(source_ips,dest_ips))
		source_ips=[]
		dest_ips=[]
		insert_db(traffic)
		counter=0
	    consumer.acknowledge(msg)

def main(args):
    logging.info('Connecting to Pulsar...')
    global counter
    # Create a pulsar client instance with reference to the broker
    client = pulsar.Client('pulsar://localhost:6650')
    logging.info('Created consumer for the topic %s', SNIFFER_TOPIC)
    consumer = client.subscribe(SNIFFER_TOPIC, SUBSCRIPTION, receiver_queue_size=100000, message_listener=traffic_listener)
    while True:
	print counter
if __name__ == '__main__':
    main(sys.argv)

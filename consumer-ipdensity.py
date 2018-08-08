import logging
import sys
import os, pulsar, mysql.connector

SNIFFER_TOPIC = 'persistent://sample/standalone/ns1/ip_sniffer'
SUBSCRIPTION = 'sub'
TIMEOUT = 10000
# Setup up basic logging
logging.basicConfig(format='%(asctime)s %(levelname)s : %(message)s', level=logging.DEBUG)

def insert_db(source,dest):
    mydb=mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="mysql",
        database="cids"
    )
    mycursor = mydb.cursor()

    sql = "INSERT INTO ip_sniffer (source_ip, dest_ip) VALUES (%s, %s)"
    val = (source, dest)
    mycursor.execute(sql, val)

    mydb.commit()

    print(mycursor.rowcount, "record inserted.")

def main(args):
    logging.info('Connecting to Pulsar...')

    # Create a pulsar client instance with reference to the broker
    client = pulsar.Client('pulsar://localhost:6650')

    consumer = client.subscribe(SNIFFER_TOPIC, SUBSCRIPTION)
    logging.info('Created consumer for the topic %s', SNIFFER_TOPIC)
    while True:
        try:
            # try and receive messages with a timeout of 10 seconds
            msg = consumer.receive(timeout_millis=TIMEOUT)
            insert_db(msg.data().split("|")[0],msg.data().split("|")[1])
            logging.info("Received message '%s'", msg.data())
    	    consumer.acknowledge(msg)  # send ack to pulsar for message consumption
        except Exception:
	        received = 0

if __name__ == '__main__':
    main(sys.argv)

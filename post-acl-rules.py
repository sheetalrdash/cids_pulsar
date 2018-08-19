import logging
import sys
import os, pulsar
import mysql.connector
import datetime

ATTACKERS_TOPIC = 'persistent://sample/standalone/ns1/attacker'
SUBSCRIPTION = 'sub1'
TIMEOUT = 10000
# Setup up basic logging
logging.basicConfig(format='%(asctime)s %(levelname)s : %(message)s', level=logging.DEBUG)


def main(args):
    mydb=mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="mysql",
        database="cids"
    )
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

            mycursor = mydb.cursor()
            mycursor.execute("SELECT blocked_ip FROM acl_retention where blocked_ip='"+msg.data()+"'")
            check_acl = mycursor.fetchall()
            mycursor.close()

            if not check_acl:
                command='curl -X POST -d \'{"src-ip":"'+msg.data()+'/32","action":"deny"}\' localhost:18080/wm/acl/rules/json'
                os.system(command)
                logging.info("Post Complete")

                # Insert into Destination Sniffer
                mycursor = mydb.cursor()
                sql = "insert into acl_retention(blocked_ip,loadtime) values('"+msg.data()+"',now())"
                mycursor.execute(sql)
                mydb.commit()
                print("{0} : {1} IP inserted into ACL Retention.".format(
                    datetime.datetime.now().strftime("%I:%M%p %d-%b-%Y"), str(msg.data())))
                mycursor.close()

            consumer.acknowledge(msg)  # send ack to pulsar for message consumption
        except Exception:
            received = 0

if __name__ == '__main__':
    main(sys.argv)

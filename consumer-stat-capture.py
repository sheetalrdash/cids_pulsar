
import sys, logging
import mysql.connector
import time, datetime
import pulsar

def pulsar_publish(attacker_ip):
    ATTACKERS_TOPIC = 'persistent://sample/standalone/ns1/attacker'
    # Setup for basic logging
    logging.basicConfig(format='%(asctime)s %(levelname)s : %(message)s', level=logging.INFO)
    logging.info('Connecting to Pulsar...')

    # Create a pulsar client instance with reference to the broker
    client = pulsar.Client('pulsar://localhost:6650')

    # Build a producer instance on a specific topic
    producer = client.create_producer(ATTACKERS_TOPIC)
    logging.info('Connected to Pulsar')

    logging.info('Sending ACL Addition Request: %s ', attacker_ip)
    producer.send(attacker_ip)

    client.close()

def insert_db():
    mydb=mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="mysql",
        database="cids"
    )
    block_list= []
    # Insert into Source Sniffer
    mycursor = mydb.cursor()
    sql = "insert into source_sniffer(source_ip,count_per_polling_interval,loadtime) (select source_ip,count(1),now() from ip_sniffer group by source_ip)"
    mycursor.execute(sql)
    mydb.commit()
    print(datetime.datetime.now().strftime("%I:%M%p %d-%b-%Y")+" : " + str(mycursor.rowcount) + " record inserted to Source_Sniffer.")
    mycursor.close()
    mycursor = mydb.cursor()

    # Insert into Destination Sniffer
    sql = "insert into dest_sniffer(dest_ip,count_per_polling_interval,loadtime) (select dest_ip,count(1),now() from ip_sniffer group by dest_ip)"
    mycursor.execute(sql)
    mydb.commit()
    print(datetime.datetime.now().strftime("%I:%M%p %d-%b-%Y")+" : " + str(mycursor.rowcount) + " record inserted to Dest_Sniffer.")
    mycursor.close()

    # Pick Source Ips with count > threshold
    mycursor = mydb.cursor()
    mycursor.execute("SELECT distinct source_ip FROM source_sniffer where count_per_polling_interval>50")
    source_ips = mycursor.fetchall()
    mycursor.close()

    # Pick Destination Ips with count > threshold
    mycursor = mydb.cursor()
    mycursor.execute("SELECT distinct dest_ip FROM dest_sniffer where count_per_polling_interval>80")
    dest_ips = mycursor.fetchall()
    mycursor.close()

    # truncate source_sniffer
    mycursor = mydb.cursor()
    sql = "truncate source_sniffer"
    mycursor.execute(sql)
    mydb.commit()
    print(datetime.datetime.now().strftime("%I:%M%p %d-%b-%Y") + " : Truncate complete for Source Sniffer")
    mycursor = mydb.cursor()

    # truncate destination sniffer
    sql = "truncate dest_sniffer"
    mycursor.execute(sql)
    mydb.commit()
    print(datetime.datetime.now().strftime("%I:%M%p %d-%b-%Y") + " : Truncate complete for Dest Sniffer")
    mycursor.close()

    for ip in dest_ips:
        mycursor = mydb.cursor()
        ip=str(ip).translate(None,"u,'()")
        mycursor.execute("SELECT distinct source_ip FROM ip_sniffer where dest_ip='"+str(ip)+"' group by source_ip order by count(*) desc LIMIT 3")
        ips = mycursor.fetchall()
        source_ips=source_ips+ips
        mycursor.close()
    for ip in source_ips:
        if ip not in block_list:
            block_list.append(ip)


    # truncate ip_sniffer
    mycursor = mydb.cursor()
    sql = "truncate ip_sniffer"
    mycursor.execute(sql)
    mydb.commit()
    print(datetime.datetime.now().strftime("%I:%M%p %d-%b-%Y")+" : Truncate complete for IP Sniffer")
    mycursor.close()

    for attacker_ip in block_list:
        attacker_ip = str(attacker_ip).translate(None, "u,'()")
        pulsar_publish(attacker_ip)

def main(args):
    while True:
        insert_db()
        time.sleep(60)

if __name__ == '__main__':
    main(sys.argv)

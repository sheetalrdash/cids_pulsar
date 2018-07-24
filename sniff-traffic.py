from scapy.all import *
import pulsar

def pulsar_publish(message):
	SNIFFER_TOPIC = 'persistent://sample/standalone/ns1/ip_sniffer'

	# Setup for basic logging
	logging.basicConfig(format='%(asctime)s %(levelname)s : %(message)s', level=logging.INFO)
	logging.info('Connecting to Pulsar...')

	# Create a pulsar client instance with reference to the broker
	client = pulsar.Client('pulsar://localhost:6650')

	# Build a producer instance on a specific topic
	producer = client.create_producer(SNIFFER_TOPIC)
	logging.info('Connected to Pulsar')

	logging.info('Source|Target: %s ', message)
	producer.send(message);


	client.close()

def sniffPackets(packet):           # custom custom packet sniffer action method
    if not (packet.haslayer(ICMP)):
        if packet[IP].seq!=0:
            pckt_src=packet[IP].src
            pckt_dst=packet[IP].dst
            pckt_ttl=packet[IP].ttl
            print "IP Packet: %s is going to %s and has ttl value %s" % (pckt_src,pckt_dst,pckt_ttl)
            pulsar_publish(pckt_src+"|"+pckt_dst)
            return
    elif packet.haslayer(ICMP) and packet[ICMP].type != 0:
        pckt_src=packet[IP].src
        pckt_dst=packet[IP].dst
        pckt_ttl=packet[IP].ttl
        print "IP Packet: %s is going to %s and has ttl value %s" % (pckt_src,pckt_dst,pckt_ttl)
        pulsar_publish(pckt_src + "|" + pckt_dst)
        return

def main():
	print "custom packet sniffer"
	sniff(filter="ip",iface="s1-eth1",prn=sniffPackets)

if __name__ == '__main__':
	main()

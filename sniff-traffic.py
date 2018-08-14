from scapy.all import *
import pulsar, commands
from multiprocessing import Process

SNIFFER_TOPIC = 'persistent://sample/standalone/ns1/ip_sniffer'
# Setup for basic logging
logging.basicConfig(format='%(asctime)s %(levelname)s : %(message)s', level=logging.INFO)
logging.info('Connecting to Pulsar...')
# Create a pulsar client instance with reference to the broker
client = pulsar.Client('pulsar://localhost:6650')

logging.info('Connected to Pulsar')
def callBack(res, msg):
    print('Message published: %s'%res)
def sniffPacketsWrapper(producer):
    def sniffPackets(packet):  # custom custom packet sniffer action method
        if not (packet.haslayer(ICMP)):
            if packet[IP].seq != 0:
                pckt_src = packet[IP].src
                pckt_dst = packet[IP].dst
                pckt_ttl = packet[IP].ttl
                print
                "IP Packet: %s is going to %s and has ttl value %s" % (pckt_src, pckt_dst, pckt_ttl)
                producer.send_async(pckt_src + "|" + pckt_dst, callBack)
                return
        elif packet.haslayer(ICMP) and packet[ICMP].type != 0:
            pckt_src = packet[IP].src
            pckt_dst = packet[IP].dst
            pckt_ttl = packet[IP].ttl
            print
            "IP Packet: %s is going to %s and has ttl value %s" % (pckt_src, pckt_dst, pckt_ttl)
            # logging.info('Source|Target: %s ', pckt_src + "|" + pckt_dst)
            producer.send_async(pckt_src + "|" + pckt_dst, callBack)
            return
    return sniffPackets

def sniffPacketsSwitch(switch):
    # Build a producer instance on a specific topic
    producer = client.create_producer(SNIFFER_TOPIC, max_pending_messages=100000)
    sniff(filter="ip",iface=switch,prn=sniffPacketsWrapper(producer))

def main():

    print "custom packet sniffer"
    switches=commands.getoutput('ifconfig|egrep "s?-eth*"|awk -F":" \'{print $1}\'')
    for switch in switches.splitlines():
        if switch != '0':
            p=Process(target=sniffPacketsSwitch, args=(switch,))
            p.start()
if __name__ == '__main__':
    main()

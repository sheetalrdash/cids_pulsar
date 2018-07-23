import requests
import time
import pulsar
import logging
import sys,threading


def topology_info():
	print('\n<----------------------------SUMMARY------------------------------>')
	data1 = requests.get("http://127.0.0.10:18080/wm/core/controller/summary/json")
	dat1 = data1.json()
	number_of_switches = dat1 ["# Switches"]
	number_of_hosts = dat1 ["# hosts"]
	print '# Switches Connected: ', number_of_switches
	print '# Hosts Connected: ', number_of_hosts
	print '------------------------------------------------------------------\n'
	return( number_of_switches , number_of_hosts )

def switch_info ( number_of_switches , switch_dpids ):
	data = requests.get("http://127.0.0.1:18080/wm/core/controller/switches/json")
	dat = data.json()
	k=0
	for k in range( 0 , number_of_switches):
		DPID = dat[k]["switchDPID"]
		DPID = DPID.decode()
		switch_dpids.append(DPID)
		bandwidth_one ( DPID, number_of_switches )
	return (switch_dpids)

def host_info(number_of_hosts , hosts):
	a = requests.get("http://127.0.0.1:18080/wm/device/")
	b = a.json()
	for k in range( 0, number_of_hosts):		
		c = str(b["devices"][k]['ipv4'])
		if c != []:			
			hosts[str(c)] = str(b["devices"][k]["attachmentPoint"])
	return(hosts)

def find_host( DPID , PORT):
    cntrlr_summ = requests.get("http://127.0.0.1:18080/wm/core/controller/summary/json")
    cntrlr_summ_json = cntrlr_summ.json()
    number_of_hosts = cntrlr_summ_json["# hosts"]
    device_detail = requests.get("http://127.0.0.1:18080/wm/device/")
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
            device_ipv4="1"
    return device_ipv4

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
	producer.send(attacker_ip);time.sleep(10)


	client.close()


def bandwidth_one ( DPID , number_of_switches ):
	print DPID
	for port in range(1,7):
		try:				
			a = requests.get("http://127.0.0.1:18080/wm/statistics/bandwidth/"+DPID+"/"+str(port)+"/json")
			b = a.json()
			acl = requests.get("http://127.0.0.1:18080/wm/acl/rules/json")
			acl_json = str(acl.json())
			bandwidth = int (b[0]["bits-per-second-rx"])
			dpid=str(b[0]["dpid"])
			ipv4=find_host(dpid, str(b[0]["port"]))
			print "\tport :", b[0]["port"]
			print "\t\tBits per Second :", bandwidth

			if bandwidth > 20000:
				if ipv4.replace('\'','') not in acl_json:
					print 'DPID: ', dpid
					print 'Port: ', str(port)
					if ipv4	!= "1":
						print "Attacker IP : ", ipv4
						pulsar_publish(ipv4)
		
		except:
			attack=False

def stat_enable():

	try:
		stat = requests.put("http://127.0.0.1:18080/wm/statistics/config/enable/json")
		print "Enabled statistics..."

	except Exception as e:
		print"error while enabling statistics :" , e

def start():

	try:

		number_of_switches , number_of_hosts = topology_info();
		switch_dpids = list()
		hosts = dict()
		attacked_switches = set()
		switch_dpids = switch_info ( number_of_switches, switch_dpids ) ;
		#hosts = host_info ( number_of_hosts , hosts );
		#dpid = switch_byte ( number_of_switches, switch_dpids );

	except Exception as e:
		print'Error occured:', e

def main():
	try:
		stat_enable();
		while 1:
			secs = 2
			time.sleep(secs)
			start()

	except:
		exit(1)

T1 = threading.Thread(target=main)
T1.start()

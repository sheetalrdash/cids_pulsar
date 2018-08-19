import logging
import sys
import os
import mysql.connector, requests, json
import datetime, time

# Setup up basic logging
logging.basicConfig(format='%(asctime)s %(levelname)s : %(message)s', level=logging.ERROR)

def main(args):
    mydb=mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="mysql",
        database="cids",
        autocommit=True
    )
    while True:
        try:
            mycursor = mydb.cursor()
            mycursor.execute("select blocked_ip from acl_retention where loadtime < now()-interval 15 minute")
            blocked_ips = mycursor.fetchall()
            mycursor.close()
            if blocked_ips:
                acl_rules = requests.get("http://127.0.0.1:18080/wm/acl/rules/json")
                acl_rules_data = acl_rules.content
                acl_rules_json = json.loads(acl_rules_data)
                for ip in blocked_ips:
                    for i in range(0, len(acl_rules_json)):
                        ip=''.join(ip)
                        if acl_rules_json[i]['nw_src'].replace("/32", "") == ip:
                            id=acl_rules_json[i]['id']
                            print("Attempting to remove IP - '" + ip + "' from ACL!!")
                            command='curl -X DELETE -d \'{"ruleid":"'+str(id)+'"}\' localhost:18080/wm/acl/rules/json'
                            os.system(command)

                            # Clean up ACL Table
                            mycursor = mydb.cursor()
                            sql = "delete from acl_retention where blocked_ip = '"+str(ip)+"'"
                            mycursor.execute(sql)
                            mydb.commit()
                            print(datetime.datetime.now().strftime("%I:%M%p %d-%b-%Y") + " : Deleted entry from ACL Retention table")

            print("\nSleeping 5 mins!")
            time.sleep(300)
        except Exception:
            received = 0

if __name__ == '__main__':
    main(sys.argv)

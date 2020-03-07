k_topic = 'datafeed'
k_servers = 'kafka-datafeed.vatsim.net:9092'
messagefilter = ['KROA_ATIS', 'KROA_APP']

import json
from datetime import datetime
from json import loads
from kafka import KafkaConsumer
from kafka.errors import BrokerNotAvailableError, NoBrokersAvailable

consumer = KafkaConsumer(k_topic,
                                bootstrap_servers=k_servers,
                                security_protocol='SASL_PLAINTEXT',
                                sasl_mechanism='PLAIN',
                                sasl_plain_username='datafeed-reader',
                                sasl_plain_password='datafeed-reader',
                                auto_offset_reset='latest',
                                value_deserializer=lambda m: loads(m.decode('utf-8')))

for message in consumer:
    message = message.value
    data = message['data']
    if message['message_type'] == 'add_client':
        # prettyprint = json.dumps(message, indent=4, separators=(',',':'))
        # print(prettyprint)
        if data['callsign'] == 'ROA_DEL':
            timestamp = str(datetime.now())
            member = data['member']
            cid = str(member['cid'])
            name = member['name']
            callsign = data['callsign']

            # prettyprint = json.dumps(message, indent=4, separators=(',',':'))
            # print(prettyprint)
            # print(data)
            print("[" + timestamp + "] - " + name  + " (" + cid + ") has opened " + callsign + " on VATSIM.")
        else:
            pass
    else:
        pass
    if message['message_type'] == 'remove_client':
        if data['callsign'] == 'ROA_DEL':
            timestamp = str(datetime.now())
            member = data['member']
            cid = str(member['cid'])
            name = member['name']
            callsign = data['callsign']

            prettyprint = json.dumps(message, indent=4, separators=(',',':'))
            print(prettyprint)
            # print(data)
            print("[" + timestamp + "] - " + name  + " (" + cid + ") has closed " + callsign + " on VATSIM.")
        else:
            pass
    else:
        pass

# REST OF CODE
print (data)
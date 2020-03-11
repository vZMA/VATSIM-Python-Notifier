# Script created to evaluate VATSIM network logins and logouts to relay to Discord bots
# Written by: Aaron Albertson
# Copywritten - 2020 Aaron Albertson
# Version: 1.0 - 3/11/2020

import os
from dotenv import load_dotenv
from datetime import datetime
from json import loads
from kafka import KafkaConsumer
from kafka.errors import BrokerNotAvailableError, NoBrokersAvailable
from discord_webhook import DiscordWebhook, DiscordEmbed

# Variables
k_topic = 'datafeed'
k_servers = 'kafka-datafeed.vatsim.net:9092'
load_dotenv()
webhookurl = 'https://discordapp.com/api/webhooks/687350322326405199/PTFFycoZN7q4VPp1pOvxqkgZn2RRoktQN6V6wVjrNqPBRbNaT0f89wLlxcu8_GmdVx-O'
global rating_long
# Check message filter here to meet last 4 characters of what to eval in add_clients / remove_clients
messagefilter = ['_CTR', '_APP', '_DEP', '_TWR', '_GND', '_DEL', '_FSS']
# Defs


def discord_webhook(callsign, name, cid, rating_long, server, status):

    webhook = DiscordWebhook(url=webhookurl)
    timestamp = str(datetime.now())

    if status == "online":
        embed = DiscordEmbed(title=callsign + " - Online", color=65290)
        embed.set_footer(text='ZDC VATSIM Notify Bot', icon_url='https://vzdc.org/photos/discordbot.png')
        # embed.set_thumbnail(url='https://vzdc.org/photos/logo.png')
        # embed.set_timestamp()
        embed.add_embed_field(name='Name', value=name)
        embed.add_embed_field(name='Rating', value=rating_long)
        embed.add_embed_field(name='CID', value=cid)
        embed.add_embed_field(name='Callsign', value=callsign)
        embed.add_embed_field(name='Server', value=server)

        webhook.add_embed(embed)
        response = webhook.execute()
    else:
        embed = DiscordEmbed(title=callsign + " - Offline", description=callsign + ' is now offline on the VATSIM network.', color=16711683)
        embed.set_footer(text='ZDC VATSIM Notify Bot', icon_url='https://vzdc.org/photos/discordbot.png')
        embed.set_thumbnail(url='https://vzdc.org/photos/logo.png')
        embed.set_timestamp()
        # embed.add_embed_field(name='Name', value=name)
        # embed.add_embed_field(name='Rating', value=rating_long)
        # embed.add_embed_field(name='CID', value=cid)
        # embed.add_embed_field(name='Callsign', value=callsign)
        # embed.add_embed_field(name='Server', value=server)

        webhook.add_embed(embed)
        response = webhook.execute()
        pass


def vatsim_rating_checker(rating):
    global rating_long
    if rating == 1:
        rating_long = "OBS"
    if rating == 2:
        rating_long = "S1"
    if rating == 3:
        rating_long = "S2"
    if rating == 4:
        rating_long = "S3"
    if rating == 5:
        rating_long = "C1"
    if rating == 6:
        rating_long = "C2"
    if rating == 7:
        rating_long = "C3"
    if rating == 8:
        rating_long = "I1"
    if rating == 9:
        rating_long = "I2"
    if rating == 10:
        rating_long = "I3"
    if rating == 11:
        rating_long = "SUP"
    if rating == 12:
        rating_long = "ADM"
    else:
        pass


def vatsim_notifier():
    # Call data from VATSIM Kafka Servers
    consumer = KafkaConsumer(k_topic,
                             bootstrap_servers=k_servers,
                             security_protocol='SASL_PLAINTEXT',
                             sasl_mechanism='PLAIN',
                             sasl_plain_username='datafeed-reader',
                             sasl_plain_password='datafeed-reader',
                             auto_offset_reset='latest',
                             value_deserializer=lambda m: loads(m.decode('utf-8')))

    # Notify Console script started
    timestamp = str(datetime.now())
    print("[" + timestamp + "] - Notifer Started!")
    # Evaluate results for callsign sign in and outs. FOR LOOP
    for message in consumer:
        timestamp = str(datetime.now())
        message = message.value
        data = message['data']
        if message['message_type'] == 'add_client':
            callsign = data['callsign']
            # strip callsign to last 4 characters for comparing / filtering
            strippedcall = callsign[-4:]
            # strip callsign to last 4 characters to filter out ATIS
            atischecker = callsign[-4:]
            # print("DEBUG: STRIPPED CALL: " + strippedcall)
            # print("DEBUG: ATISCHECKER: " + atischecker)
            if strippedcall in messagefilter and atischecker != "ATIS":
                timestamp = str(datetime.now())
                member = data['member']
                cid = str(member['cid'])
                name = member['name']
                callsign = data['callsign']
                rating = data['rating']
                vatsim_rating_checker(rating)
                server = data['server']
                status = "online"
                # prettyprint = json.dumps(message, indent=4, separators=(',',':'))
                # print(prettyprint)
                # print(data)
                print("[" + timestamp + "] - " + name + "[" + rating_long + "] (" + cid + ") has opened " + callsign + " on VATSIM.")
                discord_webhook(callsign, name, cid, rating_long, server, status)
            else:
                pass
        else:
            pass
        # Disabled per Seth's request
        # if message['message_type'] == 'remove_client':
        #     callsign = data['callsign']
        #     # strip callsign to first 4 characters for comparing / filtering
        #     strippedcall = callsign[:4]
        #     # strip callsign to last 4 characters to filter out ATIS
        #     atischecker = callsign[-4:]
        #     # print("DEBUG: STRIPPED CALL: " + strippedcall)
        #     # print("DEBUG: ATISCHECKER: " + atischecker)
        #     if strippedcall in messagefilter and atischecker != "ATIS":
        #         timestamp = str(datetime.now())
        #         member = data['member']
        #         cid = str(member['cid'])
        #         name = member['name']
        #         callsign = data['callsign']
        #         rating = data['rating']
        #         vatsim_rating_checker(rating)
        #         server = data['server']
        #         status = "offline"
        #         # prettyprint = json.dumps(message, indent=4, separators=(',',':'))
        #         # print(prettyprint)
        #         # print(data)
        #         print("[" + timestamp + "] - " + callsign + " has closed.")
        #         discord_webhook(callsign, None, None, None, None, status)
        #     else:
        #         pass
        # else:
        #     pass

# RUN


vatsim_notifier()

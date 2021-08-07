# Script created to evaluate VATSIM network logins and logouts to relay to Discord bots
# Written by: Aaron Albertson
# Copywritten - 2020 Aaron Albertson
# Version: 1.0 - 3/10/2020
# Version: 1.1 - 10/14/2020 - Adjusted parsing code for new format from new kafka feed to work again.
# Version: 1.2 - 10/28/2020 - Added filter to not display _OBS callsigns in open or close messages.

import json
import os
from dotenv import load_dotenv
from datetime import datetime
from json import loads
from kafka import KafkaConsumer
from kafka.errors import BrokerNotAvailableError, NoBrokersAvailable
from discord_webhook import DiscordWebhook, DiscordEmbed

# Variables
version = "v1.2"
load_dotenv()
k_topic = os.getenv('KTOPIC')
k_servers = os.getenv('KSERVERS')
token = str(os.getenv('DISCORD_TOKEN'))
dischannel = os.getenv('TEXT_CHANNEL')
webhookurl = os.getenv('WEBHOOK_URL')
global rating_long
# Check message filter here to meet first 4 characters of what to eval in add_clients / remove_clients
zdcMessageFilter = ['DCA_', 'IAD_', 'BWI_', 'PCT_', 'ADW_', 'DC_C', 'RIC_', 'ROA_', 'ORF_', 'ACY_', 'NGU_', 'NTU_',
    'NHK_', 'RDU_', 'CHO_', 'HGR_', 'LYH_', 'EWN_', 'LWB_', 'ISO_', 'MTN_', 'HEF_', 'MRB_', 'PHF_', 'SBY_', 'NUI_',
    'FAY_', 'ILM_', 'NKT_', 'NCA_', 'NYG_', 'DAA_', 'DOV_', 'POB_', 'GSB_', 'WAL_', 'CVN_', 'DC_0', 'DC_1', 'DC_2',
    'DC_3', 'DC_5', 'DC_N', 'DC_S', 'DC_E', 'DC_W', 'DC_I', 'JYO_']
zmaMessageFilter = ['MIA_', 'TPA_', 'PBI_', 'RSW_', 'NQX_', 'FLL_', 'TMB_', 'OPF_', 'EYW_', 'SRQ_', '6FA_', 'APF_', 'BCT_',
    'BKV_', 'BOW_', 'FMY_', 'FPR_', 'FXE_', 'HST_', 'LAL_', 'MCF_', 'PIE_', 'PGD_', 'PMP_', 'SPG_', 'SUA_', 'ZMO_']
zmoMessageFilter = ['MYNN', 'CARI', 'ZMO_']
activeMessageFilter = zmaMessageFilter

# Defs
def discord_webhook(callsign, name, cid, rating_long, server, status):

    webhook = DiscordWebhook(url=webhookurl)

    if status == "online":
        # build signon message
        embed = DiscordEmbed(title=callsign + " OPEN", description=callsign + ' was opened by **' + name + ' (' + rating ')**', color=65290)
        embed.set_footer(text='VATSIM Notify Bot ' + version + ', credit: vZDC\'s Aaron Albertson', icon_url='https://vzdc.org/photos/discordbot.png')
        embed.set_thumbnail(url='https://vzdc.org/photos/logo.png')
        embed.set_timestamp()

        # send signon message
        webhook.add_embed(embed)
        webhook.execute()
        webhook.remove_embed(0)
        
    else:
        # build signoff message
        embed = DiscordEmbed(title=callsign + " CLOSED", description=callsign + ' is now closed (**' + name + '**)', color=16711683)
        embed.set_footer(text='VATSIM Notify Bot ' + version, icon_url='https://vzdc.org/photos/discordbot.png')
        embed.set_thumbnail(url='https://vzdc.org/photos/logo.png')
        embed.set_timestamp()

        # send signoff message
        webhook.add_embed(embed)
        webhook.execute()
        webhook.remove_embed(0)

ratingDictionary = { 1:= "OBS", 2:= "S1", 3:= "S2", 4:= "S3", 5:= "C1", 6:= "C2", 7:= "C3", 8:= "I1", 9:= "I2", 10: "I3", 11: "SUP", 12: "ADM" }
def getRatingFromNumber(ratingNumber):
    if ! (ratingNumber in ratingDictionary):
        return None
    return ratingDictionary[ratingNumber]


def vatsim_rating_checker(ratingNumber):
    global rating_long
    if ratingNumber == 1:
        rating_long = "OBS"
    if ratingNumber == 2:
        rating_long = "S1"
    if ratingNumber == 3:
        rating_long = "S2"
    if ratingNumber == 4:
        rating_long = "S3"
    if ratingNumber == 5:
        rating_long = "C1"
    if ratingNumber == 6:
        rating_long = "C2"
    if ratingNumber == 7:
        rating_long = "C3"
    if ratingNumber == 8:
        rating_long = "I1"
    if ratingNumber == 9:
        rating_long = "I2"
    if ratingNumber == 10:
        rating_long = "I3"
    if ratingNumber == 11:
        rating_long = "SUP"
    if ratingNumber == 12:
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
    print("[" + timestamp + "] - Notifer " + version + " Started!")
    # Evaluate results for callsign sign in and outs. FOR LOOP
    for message in consumer:
        message = message.value
        msgtype = message['$type']
        # print (message)
        if msgtype == 'VATSIM.Network.Dataserver.Dtos.AddClientDto, VATSIM.Network.Dataserver':
            callsign = message['callsign']
            # strip callsign to first 4 characters for comparing / filtering
            strippedcall = callsign[:4]
            # strip callsign to last 4 characters to filter out ATIS
            atischecker = callsign[-4:]
            # print("DEBUG: STRIPPED CALL: " + strippedcall)
            # print("DEBUG: ATISCHECKER: " + atischecker)
            if strippedcall in activeMessageFilter and atischecker != "ATIS" and atischecker != "_OBS":
                timestamp = str(datetime.now())
                cid = str(message['cid'])
                name = message['real_name']
                callsign = message['callsign']
                rating = message['rating']
                vatsim_rating_checker(rating)
                server = message['server']
                status = "online"
                # DEBUG 
                # prettyprint = json.dumps(message, indent=4, separators=(',',':'))
                # print(prettyprint)
                # print(data)
                print("[" + timestamp + "] - " + name  + "[" + rating_long + "] (" + cid + ") has opened " + callsign + " on VATSIM.")
                discord_webhook(callsign, name, cid, rating_long, server,status)
            else:
                pass
        else:
            pass
        if msgtype == 'VATSIM.Network.Dataserver.Dtos.RemoveClientDto, VATSIM.Network.Dataserver':
            # print (message)
            callsign = message['callsign']
            # strip callsign to first 4 characters for comparing / filtering
            strippedcall = callsign[:4]
            # strip callsign to last 4 characters to filter out ATIS
            atischecker = callsign[-4:]
            # print("DEBUG: STRIPPED CALL: " + strippedcall)
            # print("DEBUG: ATISCHECKER: " + atischecker)
            if strippedcall in activeMessageFilter and atischecker != "ATIS" and atischecker != "_OBS":
                timestamp = str(datetime.now())
                callsign = message['callsign']
                status = "offline"
                # DEBUG
                # prettyprint = json.dumps(message, indent=4, separators=(',',':'))
                # print(prettyprint)
                print("[" + timestamp + "] - " + callsign + " has closed.")
                discord_webhook(callsign, None, None, None, None, status)

            else:
                pass
        else:
            pass

# RUN
vatsim_notifier()

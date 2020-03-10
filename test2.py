from discord_webhook import DiscordWebhook, DiscordEmbed

webhook = DiscordWebhook(url='https://discordapp.com/api/webhooks/686978575202517022/xCUypwN39PYnrJOWMKwwg-OPfxlJc9yBPqhE1gCb7YiaEVHbEpnkjrz2yQcJ_11rOSC7')

embed = DiscordEmbed(title='PCT_DEL', description='Jared West has opened PCT_DEL.', color=65290)
embed.set_footer(text='ZDC VATSIM Notify Bot', icon_url='https://vzdc.org/photos/logo.png')
embed.set_thumbnail(url='https://vzdc.org/photos/logo.png')
#embed.set_timestamp()
embed.add_embed_field(name='ARTCC', value='ZDC')
embed.add_embed_field(name='Callsign', value='PCT_DEL')
embed.add_embed_field(name='Name', value='Jared West')
embed.add_embed_field(name='Frequency', value='121.500')

webhook.add_embed(embed)
response = webhook.execute()
import discord
import asyncio
from aleph import Connection


class DiscordClient(discord.Client):
    is_ready = False
    on_connect_callback = None
    on_disconnect_callback = None
    on_message_callback = None

    async def on_ready(self):
        self.is_ready = True
        if self.on_connect_callback is not None: self.on_connect_callback()

    async def on_disconnect(self):
        if self.on_connect_callback is not None: self.on_disconnect_callback()


class DiscordMessage(Connection):

    def __init__(self, client_id):
        super().__init__(client_id)

        self.clean_on_write = False
        self.clean_on_read = False

        self.bot_token = ""
        self.client = None

    #
    #
    async def _open(self):
        self.client = DiscordClient()

        self.client.on_connect_callback = self.__on_connect__
        self.client.on_disconnect_callback = self.__on_disconnect__

        await self.client.start(self.bot_token)

    async def _write(self, key, data):
        for d in data:
            if "msg" not in d: raise Exception("'msg' attribute not found in record: " + str(d))
            if "channel_id" not in d and "user_id" not in d:
                raise Exception("record must contain a 'channel_id' or a 'user_id' (" + str(d) + ")")

            msg = d["msg"]
            if "channel_id" in d:
                channel = self.client.get_channel(d["channel_id"])
                await channel.send(msg)

    #
    #
    def open(self):
        raise Exception()

    def close(self):
        if self.client is None: return
        asyncio.run(self.client.close())

    def is_connected(self):
        if self.client is None: return False
        return self.client.is_ready

    def write(self, key, data):
        self.__run_on_async_thread__(self._write(key, data))

import asyncio
import json
import websockets


class BrokerWebSocket:

    def __init__(self):

        self.url = "wss://broker-feed-url"

        self.connection = None

    async def connect(self):

        self.connection = await websockets.connect(self.url)

        print("Broker websocket connected")

    async def subscribe(self, symbols):

        payload = {
            "action": "subscribe",
            "symbols": symbols
        }

        await self.connection.send(json.dumps(payload))

    async def stream(self):

        while True:

            msg = await self.connection.recv()

            data = json.loads(msg)

            yield data
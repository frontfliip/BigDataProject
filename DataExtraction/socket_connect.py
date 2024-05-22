import asyncio
import websockets
import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

connections = [
    {
        "uri": "wss://www.bitmex.com/realtimeWeb?heartbeat=1&_primuscb=O-Xi5Qs",
        "message": {"op": "subscribe","args": ["instrument:CONTRACTS", "liquidation", "orderBookL2_25:ETHUSD", "trade:ETHUSD"]},
        "tag": "eth"
    },
    {
        "uri": "wss://www.bitmex.com/realtimeWeb?heartbeat=1&_primuscb=O-Xl4mj",
        "message": {"op":"subscribe","args":["instrument:CONTRACTS","liquidation","orderBookL2_25:XBTUSD","trade:XBTUSD"]},
        "tag": "btc"
    },
    {
        "uri": "wss://www.bitmex.com/realtimeWeb?heartbeat=1&_primuscb=O-XqRXG",
        "message": {"op":"subscribe","args":["instrument:CONTRACTS","liquidation","orderBookL2_25:SOLUSD","trade:SOLUSD"]},
        "tag": "sol"
    },
    {
        "uri": "wss://www.bitmex.com/realtimeWeb?heartbeat=1&_primuscb=O-XsYL8",
        "message": {"op":"subscribe","args":["instrument:CONTRACTS","liquidation","orderBookL2_25:AEVOUSDT","trade:AEVOUSDT"]},
        "tag": "aevo"
    },
    {
        "uri": "wss://www.bitmex.com/realtimeWeb?heartbeat=1&_primuscb=O-XtgXT",
        "message": {"op":"subscribe","args":["instrument:CONTRACTS","liquidation","orderBookL2_25:DOGEUSDT","trade:DOGEUSDT"]},
        "tag": "doge"
    }
]

async def connect_to_websocket(uri, message, tag):
    async with websockets.connect(uri) as websocket:
        await websocket.send(json.dumps(message))

        async for response in websocket:
            try:
                data = json.loads(response)
                tagged_message = {"tag": tag, "data": data}
                producer.send("crypto", tagged_message)
            except json.JSONDecodeError:
                pass   #In case an empty message occurs
async def main():
    tasks = [connect_to_websocket(conn["uri"], conn["message"], conn["tag"]) for conn in connections]
    await asyncio.gather(*tasks)

asyncio.get_event_loop().run_until_complete(main())

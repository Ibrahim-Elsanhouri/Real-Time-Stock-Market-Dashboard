import pika 
import asyncio
import json
import websockets 

RABBITMQ_HOST = 'localhost'
QUEUE_NAME = 'crypto_prices'
BINANCE_API_URL = 'wss://stream.binance.com:9443/ws/btcusdt@trade'

def send_to_rabbitmq(message):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.basic_publish(exchange='', routing_key=QUEUE_NAME, body=message)
    connection.close()

async def binance_ws():
    async with websockets.connect(BINANCE_API_URL) as ws:
        while True:
           # message = await websocket.recv()
            data = json.loads(await ws.recv())
            price_info={
                'symbol': data['s'],  # Symbol of the cryptocurrency
                'price': data['p'],   # Price of the cryptocurrency
                'timestamp': data['T'] # Timestamp of the price update  
            }
            send_to_rabbitmq(json.dumps(price_info)
)
            print(f"Sent to RabbitMQ: {price_info}")

if __name__ == "__main__":
   asyncio.run(binance_ws())

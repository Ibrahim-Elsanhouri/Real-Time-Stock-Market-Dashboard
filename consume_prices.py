import pika 
import json
from streamz import Stream 


RABBITMQ_HOST = 'localhost'
QUEUE_NAME = 'crypto_prices'

stream = Stream()

def process_message(message):
    data = json.loads(message)
    print(f"Received from RabbitMQ: {data}")        
    return data

stream.map(process_message)


def callback(ch, method, properties, body):
    message = body.decode('utf-8')
    print(f"Received message: {message}")
    stream.emit(message) 

def consume(): 
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=True)
    
    print('Waiting for messages. To exit press CTRL+C')
    try:
        channel.start_consuming()
        print('Started consuming messages...')

    except KeyboardInterrupt:
        print("Stopping consumption...")
        channel.stop_consuming()
    finally:
        connection.close()


    if __name__ == "__main__":
        consume()    

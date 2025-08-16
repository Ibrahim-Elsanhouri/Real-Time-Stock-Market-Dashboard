import pika 
import json 
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from collections import deque

RABBITMQ_HOST = 'localhost' 
QUEUE_NAME = 'crypto_prices' 
prices = deque(maxlen=20)  # Store the last 100 prices for plotting

def consume_rabbitmq():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    def callback(ch, method, properties, body):
        message = json.loads(body)
        print(f"Received message: {message}")
        prices.append(float(message['price']))  # Append the price to the deque
        print(f"Received message: {message}")

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=True)
    
    print('Waiting for messages. To exit press CTRL+C')
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("Stopping consumption...")
        channel.stop_consuming()
    finally:
        connection.close()


import threading 
threading.Thread(target=consume_rabbitmq).start()


def update_plot(frame):
    plt.clf()  # Clear the current figure
    if prices:
        plt.plot(prices, label='BTC Price', color='blue')
        plt.title('Real-time BTC Price')
        plt.xlabel('Time')
        plt.ylabel('Price (USDT)')
        plt.legend()
    else:
        plt.title('No data received yet')
    plt.grid()  

ani = animation.FuncAnimation(plt.gcf(), update_plot, interval=1000)  # Update every second
plt.show()  # Display the plot

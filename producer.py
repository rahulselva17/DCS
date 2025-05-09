import pika
import json
import time
import random

def send_task_to_scheduler():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='task_queue', durable=True)

    priorities = ["high", "medium", "low"]
    
    for i in range(1, 11):
        task = {
            'task_id': i,
            'priority': priorities[i % 3],
            'deadline': 5 + i,
            'size': random.randint(1, 10),
            'timestamp': time.time()
        }

        channel.basic_publish(
            exchange='',
            routing_key='task_queue',
            body=json.dumps(task),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        print(f"[Producer] Sent task: {task}")

    connection.close()

if __name__ == "__main__":
    send_task_to_scheduler()

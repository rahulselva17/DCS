# Import necessary modules
import pika          # For connecting to RabbitMQ
import json          # For serializing task data
import time          # To add a timestamp to tasks
import random        # For generating random task sizes

def send_task_to_scheduler():
    # Establish a connection to the local RabbitMQ server
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Declare a durable queue named 'task_queue'
    # Durable=True ensures the queue remains even after RabbitMQ restarts
    channel.queue_declare(queue='task_queue', durable=True)

    # Define priority levels
    priorities = ["high", "medium", "low"]
    
    # Generate and send 20 tasks to the queue
    for i in range(1, 21):
        task = {
            'task_id': i,                          # Unique identifier for the task
            'priority': priorities[i % 3],         # Assign priority cyclically (high, medium, low)
            'deadline': 5 + i,                     # Sample deadline value (can be adjusted)
            'size': random.randint(1, 10),         # Random task size between 1 and 10
            'timestamp': time.time()               # Record the current timestamp
        }

        # Publish the task to the queue with persistence (delivery_mode=2)
        channel.basic_publish(
            exchange='',                           # Default exchange
            routing_key='task_queue',              # Queue name
            body=json.dumps(task),                 # Convert the task dictionary to JSON string
            properties=pika.BasicProperties(
                delivery_mode=2                    # Make message persistent
            )
        )

        # Print confirmation of the sent task
        print(f"[Producer] Sent task: {task}")

    # Close the connection after sending all tasks
    connection.close()

# Run the producer if this file is executed directly
if __name__ == "__main__":
    send_task_to_scheduler()

import pika
import threading
import time
import json
import os

# RabbitMQ server host
RABBITMQ_HOST = 'localhost'

# Lists to manage active worker threads
active_worker_threads = []

# Lock to prevent race conditions while modifying worker threads
worker_lock = threading.Lock()

# Event to stop workers when required
worker_stop_event = threading.Event()

# Ensure the tracking files exist or create them if they do not
def initialize_files():
    for file in ['processing_tasks.json', 'completed_tasks.json', 'worker_count.txt']:
        if not os.path.exists(file):
            with open(file, 'w') as f:
                if 'txt' in file:
                    f.write('0')  # Initialize worker count as '0'
                else:
                    pass  # Empty JSON lines file for task tracking

# Worker function to simulate processing tasks
def worker_function(worker_id):
    def callback(ch, method, properties, body):
        try:
            # Decode the message body as a JSON object
            task = json.loads(body)
            tasks = task if isinstance(task, list) else [task]

            for t in tasks:
                # Log to processing_tasks.json
                with open('processing_tasks.json', 'a') as f:
                    f.write(json.dumps(t) + '\n')

                # Simulate task processing
                print(f"[Worker {worker_id}] Processing task:")
                for k, v in t.items():
                    print(f"  {k}: {v}")
                size = t.get('size', 1)
                print(f"[Worker {worker_id}] Simulating work for {size} second(s)...")
                time.sleep(size)  # Simulate work by sleeping
                print(f"[Worker {worker_id}] Task complete.")
                print("-" * 30)

                # Log to completed_tasks.json after completion
                with open('completed_tasks.json', 'a') as f:
                    f.write(json.dumps(t) + '\n')

            # Remove processed tasks from processing_tasks.json
            try:
                with open('processing_tasks.json', 'r') as f:
                    lines = f.readlines()
                with open('processing_tasks.json', 'w') as f:
                    f.writelines([line for line in lines if json.loads(line.strip()) not in tasks])
            except Exception as e:
                print(f"[Worker {worker_id}] Error updating processing_tasks.json: {e}")

            # Acknowledge the task is completed
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except json.JSONDecodeError:
            print(f"[Worker {worker_id}] Failed to decode message: {body}")
            ch.basic_ack(delivery_tag=method.delivery_tag)

    # Establish connection with RabbitMQ server
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    channel = connection.channel()
    # Declare the worker queue (ensure it exists)
    channel.queue_declare(queue='worker_queue', durable=True)

    # Set QoS to limit the number of messages sent to workers
    channel.basic_qos(prefetch_count=1)
    # Start consuming tasks from the queue
    channel.basic_consume(queue='worker_queue', on_message_callback=callback)

    print(f"[Worker {worker_id}] Waiting for tasks...")

    # Loop to keep the worker active until stop event is triggered
    while not worker_stop_event.is_set():
        try:
            connection.process_data_events(time_limit=1)  # Process any messages in the queue
        except pika.exceptions.AMQPConnectionError:
            break

    print(f"[Worker {worker_id}] Stopping worker node {worker_id}...")

# Function to monitor the length of the task queue
def monitor_queue():
    global active_worker_threads
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    channel = connection.channel()
    # Declare the worker queue (ensure it exists)
    channel.queue_declare(queue='worker_queue', durable=True)

    while True:
        # Check the current length of the queue
        queue = channel.queue_declare(queue='worker_queue', durable=True, passive=True)
        queue_length = queue.method.message_count
        print(f"[Manager] Queue length: {queue_length} | Active workers: {len(active_worker_threads)}")

        # Monitor worker threads and adjust based on queue length
        with worker_lock:
            # Add more workers if there are more tasks in the queue than available workers
            if queue_length > len(active_worker_threads) and len(active_worker_threads) < 10:
                new_worker_thread = threading.Thread(
                    target=worker_function,
                    args=(len(active_worker_threads) + 1,),  # Assign worker id
                    daemon=True
                )
                new_worker_thread.start()  # Start the new worker
                active_worker_threads.append(new_worker_thread)
                print(f"[Manager] Added new worker. Total workers: {len(active_worker_threads)}")

            # Stop workers if the queue is empty and there are extra workers
            elif queue_length == 0 and len(active_worker_threads) > 1:
                print("[Manager] Queue is empty. Stopping idle workers.")
                worker_stop_event.set()  # Set the event to stop workers
                active_worker_threads.clear()  # Clear active worker list

            # Update the number of workers in worker_count.txt
            with open("worker_count.txt", "w") as f:
                f.write(str(len(active_worker_threads)))

        # Wait for 3 seconds before checking again
        time.sleep(3)

# Main execution
if __name__ == '__main__':
    # Initialize necessary files if they do not exist
    initialize_files()

    # Start initial workers (3 in this case)
    num_workers = 3
    for i in range(num_workers):
        thread = threading.Thread(target=worker_function, args=(i + 1,), daemon=True)
        thread.start()  # Start worker thread
        active_worker_threads.append(thread)

    # Start monitoring the task queue
    monitor_queue()

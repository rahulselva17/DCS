import pika
import threading
import time
import json
import os

RABBITMQ_HOST = 'localhost'
active_worker_threads = []
worker_lock = threading.Lock()
worker_stop_event = threading.Event()

# Ensure the tracking files exist
def initialize_files():
    for file in ['processing_tasks.json', 'completed_tasks.json', 'worker_count.txt']:
        if not os.path.exists(file):
            with open(file, 'w') as f:
                if 'txt' in file:
                    f.write('0')
                else:
                    pass  # Empty JSON lines file

# Function to simulate a worker
def worker_function(worker_id):
    def callback(ch, method, properties, body):
        try:
            task = json.loads(body)
            tasks = task if isinstance(task, list) else [task]

            for t in tasks:
                # Log to processing_tasks.json
                with open('processing_tasks.json', 'a') as f:
                    f.write(json.dumps(t) + '\n')

                print(f"[Worker {worker_id}] Processing task:")
                for k, v in t.items():
                    print(f"  {k}: {v}")
                size = t.get('size', 1)
                print(f"[Worker {worker_id}] Simulating work for {size} second(s)...")
                time.sleep(size)
                print(f"[Worker {worker_id}] Task complete.")
                print("-" * 30)

                # Log to completed_tasks.json
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

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except json.JSONDecodeError:
            print(f"[Worker {worker_id}] Failed to decode message: {body}")
            ch.basic_ack(delivery_tag=method.delivery_tag)

    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue='worker_queue', durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='worker_queue', on_message_callback=callback)

    print(f"[Worker {worker_id}] Waiting for tasks...")

    while not worker_stop_event.is_set():
        try:
            connection.process_data_events(time_limit=1)
        except pika.exceptions.AMQPConnectionError:
            break

    print(f"[Worker {worker_id}] Stopping worker node {worker_id}...")

# Function to monitor the task queue length
def monitor_queue():
    global active_worker_threads
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue='worker_queue', durable=True)

    while True:
        queue = channel.queue_declare(queue='worker_queue', durable=True, passive=True)
        queue_length = queue.method.message_count
        print(f"[Manager] Queue length: {queue_length} | Active workers: {len(active_worker_threads)}")

        with worker_lock:
            if queue_length > len(active_worker_threads) and len(active_worker_threads) < 10:
                new_worker_thread = threading.Thread(
                    target=worker_function,
                    args=(len(active_worker_threads) + 1,),
                    daemon=True
                )
                new_worker_thread.start()
                active_worker_threads.append(new_worker_thread)
                print(f"[Manager] Added new worker. Total workers: {len(active_worker_threads)}")

            elif queue_length == 0 and len(active_worker_threads) > 1:
                print("[Manager] Queue is empty. Stopping idle workers.")
                worker_stop_event.set()
                active_worker_threads.clear()

            with open("worker_count.txt", "w") as f:
                f.write(str(len(active_worker_threads)))

        time.sleep(3)

if __name__ == '__main__':
    initialize_files()

    num_workers = 3
    for i in range(num_workers):
        thread = threading.Thread(target=worker_function, args=(i + 1,), daemon=True)
        thread.start()
        active_worker_threads.append(thread)

    monitor_queue()

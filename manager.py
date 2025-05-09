import pika
import threading
import time
import json

RABBITMQ_HOST = 'localhost'
active_worker_threads = []  # Track active workers
worker_lock = threading.Lock()  # To manage worker creation and removal safely
worker_stop_event = threading.Event()  # Event to signal workers to stop

# Function to simulate a worker
def worker_function(worker_id):
    def callback(ch, method, properties, body):
        try:
            task = json.loads(body)

            # Handle list of tasks or single task
            if isinstance(task, list):
                for t in task:
                    print(f"[Worker {worker_id}] Processing task:")
                    for k, v in t.items():
                        print(f"  {k}: {v}")
                    size = t.get('size', 1)
                    print(f"[Worker {worker_id}] Simulating work for {size} second(s)...")
                    time.sleep(size)
                    print(f"[Worker {worker_id}] Task complete.")
                    print("-" * 30)

            elif isinstance(task, dict):
                print(f"[Worker {worker_id}] Processing task:")
                for k, v in task.items():
                    print(f"  {k}: {v}")
                size = task.get('size', 1)
                print(f"[Worker {worker_id}] Simulating work for {size} second(s)...")
                time.sleep(size)
                print(f"[Worker {worker_id}] Task complete.")
                print("-" * 30)

            else:
                print(f"[Worker {worker_id}] Unexpected task format: {type(task)}")

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

        # Dynamically scale worker threads
        with worker_lock:
            if queue_length > len(active_worker_threads) and len(active_worker_threads) < 10:
                # Add new worker
                new_worker_thread = threading.Thread(
                    target=worker_function,
                    args=(len(active_worker_threads) + 1,),
                    daemon=True
                )
                new_worker_thread.start()
                active_worker_threads.append(new_worker_thread)
                print(f"[Manager] Added new worker. Total workers: {len(active_worker_threads)}")

            elif queue_length == 0 and len(active_worker_threads) > 1:
                # Stop workers if queue is empty
                print("[Manager] Queue is empty. Stopping idle workers.")
                worker_stop_event.set()  # Signal workers to stop
                active_worker_threads.clear()

        time.sleep(3)

if __name__ == '__main__':
    num_workers = 3

    # Start initial workers
    for i in range(num_workers):
        thread = threading.Thread(target=worker_function, args=(i + 1,), daemon=True)
        thread.start()
        active_worker_threads.append(thread)

    monitor_queue()

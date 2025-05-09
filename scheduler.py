import pika
import json
import time
import threading

# Define priority values
priority_order = {'high': 1, 'medium': 2, 'low': 3}
task_queue = []
BATCH_SIZE = 10
LOCK = threading.Lock()

# Function to sort tasks by size > priority > age > deadline
def update_task_priority():
    task_queue.sort(key=lambda x: (
        -x['size'],
        priority_order.get(x['priority'], 3),
        time.time() - x['timestamp'],
        x['deadline']
    ))

# Function to dispatch the sorted tasks to worker_queue
def dispatch_tasks():
    global task_queue
    with LOCK:
        if not task_queue:
            return

        update_task_priority()

        print("[Scheduler] Dispatching sorted task queue to worker_queue:")
        for t in task_queue:
            age = round(time.time() - t['timestamp'], 2)
            print(f"  - Task {t['task_id']} | Size: {t['size']} | Priority: {t['priority']} | Deadline: {t['deadline']} | Age: {age}s")

        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        channel.queue_declare(queue='worker_queue', durable=True)

        for t in task_queue:
            channel.basic_publish(
                exchange='',
                routing_key='worker_queue',
                body=json.dumps(t),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            print(f"[Scheduler] Forwarded task {t['task_id']} to worker_queue")

        connection.close()
        task_queue = []  # Clear after dispatch

# Timer-based fallback dispatcher (in case batch is not reached)
def timer_dispatch():
    while True:
        time.sleep(5)  # Every 5 seconds, check and dispatch if needed
        with LOCK:
            if task_queue:
                dispatch_tasks()

# Callback when message is received
def callback(ch, method, properties, body):
    task = json.loads(body)
    task['timestamp'] = time.time()

    if task['priority'] not in priority_order:
        task['priority'] = 'low'

    with LOCK:
        task_queue.append(task)

    print(f"[Scheduler] Received task {task['task_id']} | Priority: {task['priority']} | Size: {task['size']}")

    if len(task_queue) >= BATCH_SIZE:
        dispatch_tasks()

    ch.basic_ack(delivery_tag=method.delivery_tag)

# Start consuming from task_queue
def start_scheduler():
    # Start fallback dispatcher thread
    threading.Thread(target=timer_dispatch, daemon=True).start()

    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='task_queue', durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='task_queue', on_message_callback=callback)

    print("[Scheduler] Waiting for tasks...")
    channel.start_consuming()

if __name__ == "__main__":
    start_scheduler()

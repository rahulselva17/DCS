# Import necessary libraries
import pika              # For interacting with RabbitMQ
import json              # To handle JSON serialization of tasks
import time              # For timestamps and sleep functionality
import threading         # To handle background tasks (like timer_dispatch)

# Define priority values where lower values indicate higher priority
priority_order = {'high': 1, 'medium': 2, 'low': 3}
task_queue = []  # List to store tasks waiting to be processed
BATCH_SIZE = 10  # Size of batch to dispatch to worker_queue
LOCK = threading.Lock()  # Thread lock to ensure thread safety when accessing task_queue

# Function to update the task queue's priority
# Sort the tasks by: size (desc), priority (asc), age (asc), deadline (asc)
def update_task_priority():
    task_queue.sort(key=lambda x: (
        -x['size'],  # Size: Larger tasks have higher priority
        priority_order.get(x['priority'], 3),  # Priority: 'high' is most urgent
        time.time() - x['timestamp'],  # Age: Older tasks should be processed first
        x['deadline']  # Deadline: Tasks with earlier deadlines come first
    ))

# Function to dispatch tasks to the worker_queue
def dispatch_tasks():
    global task_queue
    with LOCK:  # Use lock to ensure thread-safe access to the task_queue
        if not task_queue:
            return

        # Sort tasks by defined priority (size, priority, age, deadline)
        update_task_priority()

        print("[Scheduler] Dispatching sorted task queue to worker_queue:")
        for t in task_queue:
            age = round(time.time() - t['timestamp'], 2)  # Calculate task age
            print(f"  - Task {t['task_id']} | Size: {t['size']} | Priority: {t['priority']} | Deadline: {t['deadline']} | Age: {age}s")

        # Establish connection to RabbitMQ and set up the worker queue
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        channel.queue_declare(queue='worker_queue', durable=True)  # Durable queue ensures persistence

        # Forward each task to the worker queue
        for t in task_queue:
            channel.basic_publish(
                exchange='',
                routing_key='worker_queue',  # Send tasks to worker_queue
                body=json.dumps(t),  # Serialize the task to JSON format
                properties=pika.BasicProperties(delivery_mode=2)  # Ensure message persistence
            )
            print(f"[Scheduler] Forwarded task {t['task_id']} to worker_queue")

        # Close the connection after sending all tasks
        connection.close()

        # Clear the task queue after dispatching
        task_queue = []

# Timer-based fallback dispatcher to ensure tasks are dispatched even if batch size is not met
def timer_dispatch():
    while True:
        time.sleep(5)  # Wait 5 seconds before checking task queue
        with LOCK:  # Ensure thread-safe access to task_queue
            if task_queue:
                dispatch_tasks()  # Dispatch tasks if any are in the queue

# Callback function when a message (task) is received from the task_queue
def callback(ch, method, properties, body):
    task = json.loads(body)  # Deserialize the task from JSON

    # Add a timestamp to the task when it's received
    task['timestamp'] = time.time()

    # If the task has an invalid priority, assign it 'low'
    if task['priority'] not in priority_order:
        task['priority'] = 'low'

    # Add the task to the task_queue with thread safety
    with LOCK:
        task_queue.append(task)

    print(f"[Scheduler] Received task {task['task_id']} | Priority: {task['priority']} | Size: {task['size']}")

    # If the batch size has been met, immediately dispatch tasks
    if len(task_queue) >= BATCH_SIZE:
        dispatch_tasks()

    # Acknowledge that the task was received and processed
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Function to start the scheduler and begin consuming tasks from RabbitMQ
def start_scheduler():
    # Start a background thread to handle the timer-based dispatch
    threading.Thread(target=timer_dispatch, daemon=True).start()

    # Establish connection to RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Declare the task queue for receiving tasks (durable to ensure persistence)
    channel.queue_declare(queue='task_queue', durable=True)

    # Set the prefetch count to 1, ensuring fair dispatch of tasks
    channel.basic_qos(prefetch_count=1)

    # Start consuming from the task_queue, using the callback for each new message
    channel.basic_consume(queue='task_queue', on_message_callback=callback)

    print("[Scheduler] Waiting for tasks...")
    channel.start_consuming()  # Start processing tasks

# Entry point to start the scheduler when the script is executed directly
if __name__ == "__main__":
    start_scheduler()

from flask import Flask, render_template, jsonify
import json
import os

# Initialize the Flask application
app = Flask(__name__)

# Util to read task info from JSON files
def read_task_data(filename):
    # Check if the file exists, if not return an empty list
    if not os.path.exists(filename):
        return []
    
    # Open the file and read each line, parsing the JSON content
    with open(filename, 'r') as f:
        return [json.loads(line.strip()) for line in f if line.strip()]

# Route to render the dashboard page
@app.route('/')
def dashboard():
    # Render the 'dashboard.html' template
    return render_template('dashboard.html')

# API route to get task and worker data
@app.route('/api/data')
def get_data():
    # Read the processing and completed tasks from the JSON files
    processing_tasks = read_task_data('processing_tasks.json')
    completed_tasks = read_task_data('completed_tasks.json')

    # Try to read the worker count from the 'worker_count.txt' file
    try:
        with open('worker_count.txt') as f:
            worker_count = int(f.read().strip())
    except:
        # If the file doesn't exist or is malformed, set worker count to 0
        worker_count = 0

    # Return the data as a JSON response
    return jsonify({
        'processing_count': len(processing_tasks),  # Count of processing tasks
        'completed_count': len(completed_tasks),  # Count of completed tasks
        'worker_count': worker_count,  # Current number of active workers
        'processing_tasks': processing_tasks,  # List of processing tasks
        'completed_tasks': completed_tasks  # List of completed tasks
    })

# Run the Flask application in debug mode
if __name__ == '__main__':
    app.run(debug=True)

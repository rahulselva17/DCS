from flask import Flask, render_template, jsonify
import json
import os

app = Flask(__name__)

# Util to read task info
def read_task_data(filename):
    if not os.path.exists(filename):
        return []
    with open(filename, 'r') as f:
        return [json.loads(line.strip()) for line in f if line.strip()]

@app.route('/')
def dashboard():
    return render_template('dashboard.html')

@app.route('/api/data')
def get_data():
    processing_tasks = read_task_data('processing_tasks.json')
    completed_tasks = read_task_data('completed_tasks.json')

    try:
        with open('worker_count.txt') as f:
            worker_count = int(f.read().strip())
    except:
        worker_count = 0

    return jsonify({
        'processing_count': len(processing_tasks),
        'completed_count': len(completed_tasks),
        'worker_count': worker_count,
        'processing_tasks': processing_tasks,
        'completed_tasks': completed_tasks
    })

if __name__ == '__main__':
    app.run(debug=True)

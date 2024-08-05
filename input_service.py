from flask import Flask, request, jsonify, render_template
from confluent_kafka import Producer
from random import randbytes
import time
import json
import hashlib


hash_object = hashlib.sha256()

producer_config = {
    "bootstrap.servers": "localhost:9092"
}
producer = Producer(producer_config)

app = Flask(__name__)


@app.route('/create_task', methods=['GET'])
def show_creating_page():
    return render_template('create_task.html')


@app.route('/create_task', methods=['POST'])
def create_task():

    try:
        data = request.form
        task_name = data.get('task_name')
        task_duration = int(data.get('task_duration'))
        if task_name is None:
            return jsonify({"Error": "There is no process name"}), 400

        creation_time = time.time_ns()
        hash_object.update(f"{task_name}_{creation_time}".encode())
        task_hash = hash_object.hexdigest()
        kafka_message = {
            "hash": task_hash,
            "name": task_name,
            "duration": task_duration
        }
        producer.produce("task_topic", value=json.dumps(kafka_message))
        return jsonify({"message": "success"})
    except Exception as e:
        print(f"{e=}")
        return jsonify({"message": e})


if __name__ == '__main__':
    app.run('0.0.0.0', port=5000)

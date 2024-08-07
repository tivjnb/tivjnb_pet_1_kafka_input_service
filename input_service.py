from flask import Flask, request, jsonify, render_template, redirect
from confluent_kafka import Producer
from sqlalchemy import create_engine, MetaData, Table, String, Integer, Column, DateTime
from datetime import datetime
import time
import json
import hashlib

DB_NAME = 'pet_kafka_1'
producer_config = {
    "bootstrap.servers": "localhost:9092"
}
producer = Producer(producer_config)

hash_object = hashlib.sha256()
metadata = MetaData()
task_table = Table(
    'tasks', metadata,
    Column('id', Integer(), primary_key=True),
    Column('hash', String(200), nullable=False),
    Column('name', String(100), nullable=False),
    Column('duration', Integer(), nullable=False),
    Column('status', String(50), default='Created'),
    Column('created_on', DateTime(), default=datetime.now),
    Column('result', String(200))
)

engine = create_engine(f"postgresql+psycopg2://user2:123@localhost:5432/{DB_NAME}")


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
        if not task_name:
            return render_template('create_task.html', error="There is no task name")
        if len(task_name) > 100:
            return render_template('create_task.html', error="Name is too long (max 100 chars)")

        creation_time = time.time_ns()
        hash_object.update(f"{task_name}_{creation_time}".encode())
        task_hash = hash_object.hexdigest()
        kafka_message = {
            "hash": task_hash,
            "name": task_name,
            "duration": task_duration
        }
        producer.produce("task_topic", value=json.dumps(kafka_message))
        with engine.connect() as conn:
            ins = task_table.insert().values(**kafka_message)
            conn.execute(ins)
            conn.commit()

        return redirect(f"http://127.0.0.1:5000/status/{task_hash}")
    except ValueError:
        return render_template('create_task.html', error="Duration must be integer")
    except Exception as e:
        return render_template('create_task.html', error=str(e))


if __name__ == '__main__':
    app.run('0.0.0.0', port=8000)

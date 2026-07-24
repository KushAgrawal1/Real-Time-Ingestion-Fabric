from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import uuid
import random


default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2026, 7, 23, 12, 00)
}


FIRST_NAMES = ['James', 'Emma', 'Oliver', 'Sophia', 'Liam', 'Ava', 'Noah', 'Isabella',
               'William', 'Mia', 'Benjamin', 'Charlotte', 'Elijah', 'Amelia', 'Lucas']
LAST_NAMES = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller',
              'Davis', 'Wilson', 'Taylor', 'Anderson', 'Thomas', 'Jackson', 'White']
CITIES = ['London', 'Manchester', 'Birmingham', 'Leeds', 'Glasgow', 'Liverpool',
          'Bristol', 'Sheffield', 'Edinburgh', 'Cardiff']
COUNTRIES = ['United Kingdom']
GENDERS = ['male', 'female']
STREETS = ['High Street', 'Church Lane', 'Victoria Road', 'Kings Avenue', 'Queens Drive',
           'Park Street', 'Station Road', 'Mill Lane', 'Brook Way', 'Hill Road']


def generate_data():
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    gender = random.choice(GENDERS)
    city = random.choice(CITIES)
    street_num = random.randint(1, 999)
    street = random.choice(STREETS)
    postcode = f"{random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}{random.randint(1,9)} {random.randint(1,9)}{random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}{random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}"

    return {
        'id': str(uuid.uuid4()),
        'first_name': first,
        'last_name': last,
        'gender': gender,
        'address': f"{street_num} {street}, {city}, England, United Kingdom",
        'post_code': postcode,
        'email': f"{first.lower()}.{last.lower()}{random.randint(1,999)}@example.com",
        'username': f"{first.lower()}{last.lower()}{random.randint(1,9999)}",
        'dob': f"{random.randint(1950,2000)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}T00:00:00.000Z",
        'registered_date': f"{random.randint(2010,2024)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}T00:00:00.000Z",
        'phone': f"07{random.randint(100000000,999999999)}",
        'picture': f"https://randomuser.me/api/portraits/med/{'men' if gender == 'male' else 'women'}/{random.randint(1,99)}.jpg"
    }


def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging

    producer = KafkaProducer(
        bootstrap_servers=['broker:29092'],
        max_block_ms=5000,
        batch_size=65536,        # 64KB batch — sends more messages per request
        linger_ms=10,            # wait 10ms to fill batch before sending
        compression_type='gzip'  # compress batches
    )

    curr_time = time.time()
    count = 0

    while True:
        if time.time() > curr_time + 60:
            break
        try:
            data = generate_data()
            producer.send('users_created', json.dumps(data).encode('utf-8'))
            count += 1
        except Exception as e:
            logging.error(f'An error occurred: {e}')
            continue

    producer.flush()
    logging.info(f'Stream complete. Total messages sent: {count}')
    print(f'Total messages sent: {count}')


with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )
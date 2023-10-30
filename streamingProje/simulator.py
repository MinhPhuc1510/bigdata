import logging

from kafka import KafkaProducer
import json
from datetime import datetime


producer = KafkaProducer(bootstrap_servers='localhost:9094', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
for id in range(10):
    key = "key_1"
    value = {
        "lclid": str(id),
        "day": str(datetime.strptime("12/15/2011", "%m/%d/%Y")),
        "energy_max": 10.2,
        "energy_min": 10.3,
        "energy_sum": 10.4
    }
    future = producer.send('iot_02', value=value, key=b'key_1')
    try:
        result = future.get(timeout=60)
        print(result)
    except KeyError as ex:
        logging.exception(str(ex))

    producer.flush()

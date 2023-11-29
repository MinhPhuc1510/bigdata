import logging

from kafka import KafkaProducer
import json
from datetime import datetime
import pandas as pd

data_df = pd.read_csv(r'C:\Users\Minh Phuc\Documents\Thạc sĩ\bigdata\project\data\daily_dataset.csv')

producer = KafkaProducer(bootstrap_servers='localhost:9094', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
for id in range(1000):
    key = "iot_key_1"
    value = {
        "id": id,
        "lclid": data_df["LCLid"][id],
        "day": str(datetime.strptime(data_df['day'][id], "%Y-%m-%d")),
        "energy_max": data_df['energy_max'][id],
        "energy_min": data_df['energy_min'][id],
        "energy_sum": data_df['energy_sum'][id],
        "energy_median": data_df['energy_median'][id],
        "energy_std": data_df['energy_std'][id],

    }
    future = producer.send('iot_03', value=value, key=b'key_1')
    try:
        result = future.get(timeout=60)
        print(result)
    except KeyError as ex:
        logging.exception(str(ex))

    producer.flush()

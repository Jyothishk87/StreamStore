import uuid
import json
from confluent_kafka import Producer

producer_config = {
    "bootstrap.servers": "localhost:9092"
    }

producer = Producer(producer_config)

def delivery_report(err , msg):
   if err:
       print(f"Delievry report err :{err}")
   else:
       print(f"Delivered {msg.value().decode('utf-8')}")

order = {
    "order_id" : str(uuid.uuid4()),
    "user"  : "Vid",
    "item" : "Dell ",
    "quantity" : 2
    }


value = json.dumps(order).encode("utf-8")

producer.produce(
    topic="order",
    value = value,
    callback = delivery_report
    )
producer.flush()

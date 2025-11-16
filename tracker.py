import json
from confluent_kafka import Consumer

consumer_config ={
    "bootstrap.servers": "localhost:9092",
    "group.id" : "order-tracker",
    "auto.offset.reset" :"earliest"

}

consumer=Consumer(consumer_config)
consumer.subscribe(["order"])

print("consumer is running and subscribed to order topic")
try:
    while True:
     msg=consumer.poll(1.0)
     if msg is None:
        continue
     if msg.error():
        print("Error:",msg.error())
        continue
     value=msg.value().decode("utf-8")
     order=json.loads(value)

     print(f"Received order:{order['quantity']} * {order['item']} from {order['user']}")
except KeyboardInterrupt:
    print("\n Stopping consumer")

finally:
    consumer.close()







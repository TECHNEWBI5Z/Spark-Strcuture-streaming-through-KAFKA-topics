from confluent_kafka import Producer
import json

conf = {
    'bootstrap.servers': 'pkc-12576z.us-west2.gcp.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'TRKKPFPUEKDNEJP4',
    'sasl.password': '4tjakfpI9Ws3S98bEcsLarm5wgym771warCLRFn15dHgOpLa+iO92lt9qmQa58sO',
    'client.id': 'Faizan_Macbook'
}

producer = Producer(conf)

def acked(err, msg):
    if err is not None:
        print(f"Failed to deliver message: {err}: {msg}")
    else:
        msg_key = msg.key().decode('utf-8')
        msg_value = msg.value().decode('utf-8')
        print(f"Message produced key is: {msg_key} and value is: {msg_value}")

with open('/Users/patel/Desktop/project_2/data/growth_nested.json', 'r') as file:
    for line in file:
        growth = json.loads(line)  # Use loads instead of load
        GenotypeName = str(growth["GenotypeName"])
        producer.produce(
            topic="growth_data_stream",
            key=GenotypeName,
            value=line,
            callback=acked
        )
        producer.poll(1)
        producer.flush()
from kafka import KafkaConsumer, KafkaProducer
from PIL import Image
from io import BytesIO
import easyocr
import json
import numpy as np

reader = easyocr.Reader(['en'])

consumer = KafkaConsumer('pro3',
                         bootstrap_servers = ['localhost:9092'],
                         auto_offset_reset = 'earliest'
                         )

producer = KafkaProducer(acks=0,
                         compression_type='gzip',
                         bootstrap_servers=['localhost:9092'])

def on_send_success(record_metadata) : 
    print("메시지 전송 성공. Topic:", record_metadata.topic, "Partition:", record_metadata.partition, "Offset:", record_metadata.offset)

def on_send_error(e):
    print("메시지 전송 실패:", e)

def process_message(message) : 
    result = reader.readtext(message)
    return result

def switch_json(message) : 
    dic = {}
    for a, b, c in message : 
        dic[b] = a
    json_data = json.dumps(dic, cls = NpEncoder)
    return json_data

class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, (np.ndarray)):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)
    
def processincomsumer(message) : 
    img = BytesIO(message)
    img= Image.open(img).convert("RGB")
    img.save(f"pro-{message[:3]}.jpg")
    result = process_message(f"/Users/seonminbaek/practice/pro-{message[:3]}.jpg")
    result = switch_json(result)
    producer.send('cons3', result.encode('utf-8')).add_callback(on_send_success).add_errback(on_send_error)


if __name__ == '__main__' : 
    for message in consumer : 
        try : 
            processincomsumer(message.value)
            
        except Exception as e: 
            print(f"Error processing message: {e}")
        
import cv2
from kafka import KafkaProducer

def on_send_success(record_metadata) : 
    print("메시지 전송 성공. Topic:", record_metadata.topic, "Partition:", record_metadata.partition, "Offset:", record_metadata.offset)

def on_send_error(e):
    print("메시지 전송 실패:", e)
producer = KafkaProducer(acks=0, compression_type='gzip',bootstrap_servers=['localhost:9092'])

image = cv2.imread('project.png')
ret, image = cv2.imencode('.png',image)
producer.send('pro3', image.tobytes()).add_callback(on_send_success).add_errback(on_send_error)
producer.flush()

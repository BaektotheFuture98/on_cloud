from kafka import KafkaConsumer, KafkaProducer
from PIL import Image
from io import BytesIO
import os
from EasyOCR.easyocr import easyocr
import json
import numpy as np
import papago as pg

reader = easyocr.Reader(lang_list = ['ko'], recog_network = 'trocr', gpu=True)

consumer = KafkaConsumer('cluster',
                         bootstrap_servers = ['localhost:9092'],
                         auto_offset_reset = 'latest'
                         )

producer = KafkaProducer(acks=0,
                         compression_type='gzip',
                         bootstrap_servers=['localhost:9092'])

def on_send_success(record_metadata) : 
    print("메시지 전송 성공. Topic:", record_metadata.topic, "Partition:", record_metadata.partition, "Offset:", record_metadata.offset)

def on_send_error(e):
    print("메시지 전송 실패:", e)

def switch_json(message) : 
    dic_ko = {}
    dic_en = {}
    dic_jp = {}
    for a, b, c in message :
        print(f"type : {type(b)}, text : {b}")
        # 한국어, 영어, 일본어 번역
        result_jp = pg.get_translate(b, 'ja')
        result_en = pg.get_translate(b, 'en')
        result_ko = pg.get_translate(b, 'ko')
        dic_jp[result_jp] = a
        dic_en[result_en] = a
        dic_ko[result_ko] = a

    json_data_ko = json.dumps(dic_ko, cls=NpEncoder)
    json_data_en = json.dumps(dic_en, cls=NpEncoder)
    json_data_jp = json.dumps(dic_jp, cls=NpEncoder)
    return json_data_ko, json_data_en, json_data_jp

class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)
    
def processincomsumer(message):
    global reader
    img = BytesIO(message)
    #img= Image.open(img).convert("RGB")
    #img.save(f"pro.jpg")
    
    # 바운딩 박스, 텍스트, 임계값
    #result = reader.readtext(f"pro.jpg")
    result = reader.readtext(img, batch_size = 8192, output_format='json_specific_and_relative_pos')
    #os.remove(f"pro.jpg")
    
    print(result)

    re_ko, re_en, re_jp = switch_json(result)
    print(f"""re_ko : {re_ko}""")
    print(f"""re_en : {re_en}""")
    print(f"""re_jp : {re_jp}""")
    producer.send('korean', re_ko.encode('utf-8')).add_callback(on_send_success).add_errback(on_send_error)
    producer.send('english', re_en.encode('utf-8')).add_callback(on_send_success).add_errback(on_send_error)
    producer.send('japan', re_jp.encode('utf-8')).add_callback(on_send_success).add_errback(on_send_error)


if __name__ == '__main__' : 
    for message in consumer :
        try :
            processincomsumer(message.value)

        except Exception as e:
            print(f"Error processing message: {e}")


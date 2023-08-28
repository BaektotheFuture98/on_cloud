from kafka import KafkaConsumer, KafkaProducer
from PIL import Image
from io import BytesIO
import os
import easyocr
import json
import numpy as np
import papago as pg
<<<<<<< HEAD
import cv2
import base64
import traceback
import client_id_secret as sc

reader = easyocr.Reader(lang_list = ['ko', 'en'], gpu=True) # recog_network = 'trocr')

consumer = KafkaConsumer('topic',
                         bootstrap_servers = sc.KafkaConsumer_bootstrap_servers,
=======
import base64

#reader = easyocr.Reader(lang_list = ['ko','en','ja'], recog_network = 'trocr', gpu=True)
reader = easyocr.Reader(['en','ko'])

consumer = KafkaConsumer('test2',
                         bootstrap_servers = ['localhost:9092'],
>>>>>>> 0ab1d24 (SeonMin's modify)
                         auto_offset_reset = 'latest'
                         )

producer = KafkaProducer(acks='all',
                         compression_type='gzip',
<<<<<<< HEAD
                         bootstrap_servers= sc.KafkaProducer_bootstrap_servers)
print ('Model is Ready')
=======
                         bootstrap_servers=['localhost:9092'])
                        #  bootstrap_servers=['52.91.126.82:9092','34.232.53.143:9092','100.24.240.6:9092'])

>>>>>>> 0ab1d24 (SeonMin's modify)
def on_send_success(record_metadata) : 
    print("메시지 전송 성공. Topic:", record_metadata.topic, "Partition:", record_metadata.partition, "Offset:", record_metadata.offset)

def on_send_error(e):
    print("메시지 전송 실패:", e)

def switch_json(message):
    ko_message = message[:]
    en_message = message[:]
    jp_message = message[:]
    
    for i, sentence in enumerate(message):
        data = json.loads(sentence)
        bounding_box_pos = data['boxes']
        text = data['text']
        confident = data['confident']
        background_color = data['background_color']
        text_color = data['text_color']
        
        print(f"type : {type(text)}, text : {text}")
        result_jp = pg.get_translate(text, 'ja')
        result_en = pg.get_translate(text, 'en')
        result_ko = pg.get_translate(text, 'ko')
        data['text'] = result_ko
        ko_message[i] = json.dumps(data)
        
        data['text'] = result_en
        en_message[i] = json.dumps(data)
        
        data['text'] = result_jp
        jp_message[i] = json.dumps(data)
    
    # for a, b, c in message :
    #     print(f"type : {type(b)}, text : {b}")
    #     # 한국어, 영어, 일본어 번역
    #     result_jp = pg.get_translate(b, 'ja')
    #     result_en = pg.get_translate(b, 'en')
    #     result_ko = pg.get_translate(b, 'ko')
    #     dic_jp[result_jp] = a
    #     dic_en[result_en] = a
    #     dic_ko[result_ko] = a


    json_data_ko = json.dumps(ko_message, cls=NpEncoder, indent=4)
    json_data_en = json.dumps(en_message, cls=NpEncoder, indent=4)
    json_data_jp = json.dumps(jp_message, cls=NpEncoder, indent=4)
#    print ('final Result')
    print (json_data_en, json_data_ko, json_data_jp)
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
<<<<<<< HEAD
    global reader
    message = base64.b64decode(message)
    img = BytesIO(message)
    img= np.array(Image.open(img))
    img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
 #   print (type(img))
    #print (type(img))
    #img.save(f"pro.jpg")
    
    # 바운딩 박스, 텍스트, 임계값
    #result = reader.readtext(f"pro.jpg")
    result = None
    if img is not None:
        result = reader.readtext(img, decoder = 'beamsearch', beamWidth=5,width_ths=2, paragraph= False, batch_size = 10, output_format='json_specific_and_relative_pos')
        print (result)
        
    #os.remove(f"pro.jpg")
    
  #  print(result)
=======
    print(type(message))
#    # global reader
#     img = BytesIO(message)
#     img = Image.open(img).convert("RGB")
#     img.save(f"pro.jpg")
    img = base64.b64decode(message)
    # 바운딩 박스, 텍스트, 임계값
    with open('pro.png', 'bw') as f :
        f.write(img)
    result = reader.readtext(f"pro.png")

    os.remove(f"pro.png")

    print(result)
>>>>>>> 0ab1d24 (SeonMin's modify)

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
        except KeyboardInterrupt as e:
            print (e)
            break
        except Exception as e:
            print (f"Error : {e}")
            print (traceback.format_exc())


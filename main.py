from kafka import KafkaConsumer, KafkaProducer
from PIL import Image
from io import BytesIO
import os
from EasyOCR.easyocr import easyocr
import json
import numpy as np
import papago as pg
import cv2
import base64
import traceback
import client_id_secret as sc
import asyncio

reader = easyocr.Reader(lang_list = ['ko', 'en'], gpu=True, recog_network = 'trocr')

consumer = KafkaConsumer('test2',
                         #bootstrap_servers = sc.KafkaConsumer_bootstrap_servers,
                         bootstrap_servers = ['localhost:9092'],
                         auto_offset_reset = 'earliest'
                         )

producer = KafkaProducer(acks='all',
                         compression_type='gzip',
                         #bootstrap_servers= sc.KafkaProducer_bootstrap_servers)
                         bootstrap_servers = ['localhost:9092'])
print ('Model is Ready')
def on_send_success(record_metadata) : 
    print("메시지 전송 성공. Topic:", record_metadata.topic, "Partition:", record_metadata.partition, "Offset:", record_metadata.offset)

def on_send_error(e):
    print("메시지 전송 실패:", e)

# def switch_json(message):
#     ko_message = message[:]
#     en_message = message[:]
#     jp_message = message[:]
    
#     for i, sentence in enumerate(message):
#         data = json.loads(sentence)
#         bounding_box_pos = data['boxes']
#         text = data['text']
#         confident = data['confident']
#         background_color = data['background_color']
#         text_color = data['text_color']
        
#         print(f"type : {type(text)}, text : {text}")
#         result_jp = pg.get_translate(text, 'ja')
#         result_en = pg.get_translate(text, 'en')
#         result_ko = pg.get_translate(text, 'ko')
#         data['text'] = result_ko
#         ko_message[i] = json.dumps(data)
        
#         data['text'] = result_en
#         en_message[i] = json.dumps(data)
        
#         data['text'] = result_jp
#         jp_message[i] = json.dumps(data)
    


#     json_data_ko = json.dumps(ko_message, cls=NpEncoder, indent=4)
#     json_data_en = json.dumps(en_message, cls=NpEncoder, indent=4)
#     json_data_jp = json.dumps(jp_message, cls=NpEncoder, indent=4)
# #    print ('final Result')
#     print (json_data_en, json_data_ko, json_data_jp)
#     return json_data_ko, json_data_en, json_data_jp

async def switch_json_ko(message):
    ko_message = message[:]
    
    for i, sentence in enumerate(message):
        data = json.loads(sentence)
        bounding_box_pos = data['boxes']
        text = data['text']
        confident = data['confident']
        background_color = data['background_color']
        text_color = data['text_color']
        
        print(f"type : {type(text)}, text : {text}")
        
        result_ko = pg.get_translate(text, 'ko')
        data['text'] = result_ko
        ko_message[i] = json.dumps(data)

    json_data_ko = json.dumps(ko_message, cls=NpEncoder, indent=4)

#    print ('final Result')
    print (json_data_ko)
    producer.send('korean', json_data_ko.encode('utf-8')).add_callback(on_send_success).add_errback(on_send_error)
    
async def switch_json_en(message):
    en_message = message[:]
    
    for i, sentence in enumerate(message):
        data = json.loads(sentence)
        bounding_box_pos = data['boxes']
        text = data['text']
        confident = data['confident']
        background_color = data['background_color']
        text_color = data['text_color']
        
        print(f"type : {type(text)}, text : {text}")
        
        result_ko = pg.get_translate(text, 'en')
        data['text'] = result_ko
        en_message[i] = json.dumps(data)

    json_data_en = json.dumps(en_message, cls=NpEncoder, indent=4)

#    print ('final Result')
    print (json_data_en)
    producer.send('english', json_data_en.encode('utf-8')).add_callback(on_send_success).add_errback(on_send_error)
    
async def switch_json_jp(message):
    jp_message = message[:]
    
    for i, sentence in enumerate(message):
        data = json.loads(sentence)
        bounding_box_pos = data['boxes']
        text = data['text']
        confident = data['confident']
        background_color = data['background_color']
        text_color = data['text_color']
        
        print(f"type : {type(text)}, text : {text}")
        
        result_ko = pg.get_translate(text, 'ja')
        data['text'] = result_ko
        jp_message[i] = json.dumps(data)

    json_data_jp = json.dumps(jp_message, cls=NpEncoder, indent=4)

#    print ('final Result')
    print (json_data_jp)
    producer.send('japan', json_data_jp.encode('utf-8')).add_callback(on_send_success).add_errback(on_send_error)


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)
    
async def processincomsumer(message):
    global reader
    message = base64.b64decode(message)
    img = BytesIO(message)
    img= np.array(Image.open(img))
    img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
    if img is not None:
        result = reader.readtext(img, decoder = 'beamsearch', beamWidth=5, width_ths=2, paragraph= False, batch_size = 10, output_format='json_specific_and_relative_pos')
    await asyncio.gather(switch_json_en(result), switch_json_jp(result), switch_json_ko(result))
    
    # re_ko, re_en, re_jp = switch_json(result)
    # print(f"""re_ko : {re_ko}""")
    # print(f"""re_en : {re_en}""")
    # print(f"""re_jp : {re_jp}""")
    
    # producer.send('english', re_en.encode('utf-8')).add_callback(on_send_success).add_errback(on_send_error)
    # producer.send('japan', re_jp.encode('utf-8')).add_callback(on_send_success).add_errback(on_send_error)



if __name__ == '__main__' : 
    loop = asyncio.get_event_loop()
    for message in consumer :
        try :
            loop.run_until_complete(processincomsumer(message.value))
        except KeyboardInterrupt as e:
            print (e)
            break
        except Exception as e:
            print (f"Error : {e}")
            print (traceback.format_exc())


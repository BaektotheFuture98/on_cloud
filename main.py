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

consumer = KafkaConsumer('topic',
                         bootstrap_servers = sc.KafkaConsumer_bootstrap_servers,
                         auto_offset_reset = 'latest'
                         )

producer = KafkaProducer(acks='all',
                         compression_type='gzip',
                         bootstrap_servers= sc.KafkaProducer_bootstrap_servers)
                         #bootstrap_servers = ['localhost:9092'])

print ('Model is Ready')
def on_send_success(record_metadata) : 
    print("메시지 전송 성공. Topic:", record_metadata.topic, "Partition:", record_metadata.partition, "Offset:", record_metadata.offset)

def on_send_error(e):
    print("메시지 전송 실패:", e)

async def switch_json(messages, targets = ['ko', 'en', 'ja']):
    # Bounding box가 여러개 있는 메세지들의 묶음 병렬 처리 가능
    output_target_lang_messages = {lang : messages[:] for lang in targets}
    all_text = [json.loads(sentence)['text'] for sentence in messages]
    print (all_text)
    # 각각의 언어별로 번역 각 text 만 바뀌면 된다.
    result = await asyncio.gather(*list(pg.get_translate(one_word, l) for one_word in all_text for l in targets))
    print (result)
    # 원래 언어에서 target 언어로 변환 후 dictionary message 값 출력
#     messages = dict()
#     messages[target] = message[:]
#     all_text = []
#     for i, sentence in enumerate(message):
#         data = json.loads(sentence)
#         bounding_box_pos = data['boxes']
#         text = data['text']
#         confident = data['confident']
#         background_color = data['background_color']
#         text_color = data['text_color']
#         all_text.append(text)
    
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
    message = base64.b64decode(message)
    img = BytesIO(message)
    img= np.array(Image.open(img))
    img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
    print ('img is not None')

    if img is not None:
        print ('img is not None')
        result = reader.readtext(img, decoder = 'beamsearch', beamWidth=5, width_ths=2, paragraph= False, batch_size = 10, output_format='json_specific_and_relative_pos')
    #await asyncio.gather(switch_json_en(result), switch_json_jp(result), switch_json_ko(result))
    
    re_ko, re_en, re_jp = switch_json(result)
    print(f"""re_ko : {re_ko}""")
    print(f"""re_en : {re_en}""")
    print(f"""re_jp : {re_jp}""")

    producer.send('english', re_en.encode('utf-8')).add_callback(on_send_success).add_errback(on_send_error)
    producer.send('japan', re_jp.encode('utf-8')).add_callback(on_send_success).add_errback(on_send_error)



if __name__ == '__main__' : 
    asyncio.run(switch_json(['{"boxes": [[0.11475409836065574, 0.07890070921985816], [0.2693208430913349, 0.07890070921985816], [0.2693208430913349, 0.11790780141843972], [0.11475409836065574, 0.11790780141843972]], "text": "MeetUS 번역", "confident": 0.9, "background_color": [217, 202, 183, 127], "text_color": [61, 53, 52, 255]}', '{"boxes": [[0.117096018735363, 0.12411347517730496], [0.15768930523028885, 0.12411347517730496], [0.15768930523028885, 0.1524822695035461], [0.117096018735363, 0.1524822695035461]], "text": "요 5", "confident": 0.9, "background_color": [220, 203, 184, 127], "text_color": [159, 144, 130, 255]}', '{"boxes": [[0.20921155347384857, 0.20921985815602837], [0.27946916471506633, 0.20921985815602837], [0.27946916471506633, 0.2375886524822695], [0.20921155347384857, 0.2375886524822695]], "text": "ERROR", "confident": 0.9, "background_color": [59, 226, 243, 127], "text_color": [24, 96, 104, 255]}', '{"boxes": [[0.2068696330991413, 0.26861702127659576], [0.6893052302888368, 0.26861702127659576], [0.6893052302888368, 0.3076241134751773], [0.2068696330991413, 0.3076241134751773]], "text": "Message: respect: l2, version: 3)", "confident": 0.9, "background_color": [56, 231, 249, 127], "text_color": [26, 107, 120, 255]}', '{"boxes": [[0.20843091334894615, 0.300531914893617], [1.0, 0.300531914893617], [1.0, 0.3377659574468085], [0.20843091334894615, 0.3377659574468085]], "text": "Details: theart API is used by members of a consumer", "confident": 0.9, "background_color": [56, 231, 249, 127], "text_color": [26, 109, 122, 255]}', '{"boxes": [[0.2068696330991413, 0.3351063829787234], [0.6690085870413739, 0.3351063829787234], [0.6690085870413739, 0.3696808510638298], [0.2068696330991413, 0.3696808510638298]], "text": "this case, an error occurred white trying to send", "confident": 0.9, "background_color": [56, 231, 248, 127], "text_color": [25, 106, 118, 255]}', '{"boxes": [[0.6736924277907884, 0.3448581560283688], [0.6861826697892272, 0.3448581560283688], [0.6861826697892272, 0.3625886524822695], [0.6736924277907884, 0.3625886524822695]], "text": "a", "confident": 0.9, "background_color": [63, 223, 237, 127], "text_color": [25, 108, 123, 255]}', '{"boxes": [[0.20765027322404372, 0.40602836879432624], [0.2888368462138954, 0.40602836879432624], [0.2888368462138954, 0.43439716312056736], [0.20765027322404372, 0.43439716312056736]], "text": "전체보기", "confident": 0.9, "background_color": [59, 229, 246, 127], "text_color": [31, 122, 133, 255]}', '{"boxes": [[0.10772833723653395, 0.425531914893617], [0.19359875097580015, 0.425531914893617], [0.19359875097580015, 0.4521276595744681], [0.10772833723653395, 0.4521276595744681]], "text": "오후 6:28", "confident": 0.9, "background_color": [218, 202, 184, 127], "text_color": [127, 114, 102, 255]}', '{"boxes": [[0.20843091334894615, 0.5292553191489362], [1.0, 0.5292553191489362], [1.0, 0.5647163120567376], [0.20843091334894615, 0.5647163120567376]], "text": "Kafkals에서 나타난 위의 오류와 경고는 kafka 컨슈머", "confident": 0.9, "background_color": [57, 230, 248, 127], "text_color": [25, 103, 116, 255]}', '{"boxes": [[0.20843091334894615, 0.5611702127659575], [0.9921935987509758, 0.5611702127659575], [0.9921935987509758, 0.5975177304964538], [0.20843091334894615, 0.5975177304964538]], "text": "미션의 수 변경 또는 다른 구성원의 연결 끊김과 같은 여러 이벤트로 인해 자동", "confident": 0.9, "background_color": [57, 231, 249, 127], "text_color": [26, 105, 116, 255]}', '{"boxes": [[0.209992193598751, 0.5930851063829787], [0.8719750195160031, 0.5930851063829787], [0.8719750195160031, 0.6285460992907801], [0.209992193598751, 0.6285460992907801]], "text": "이 너무 자주 발생하면 그 이유를 파악하고 해결하는 것이 중요합니다.", "confident": 0.9, "background_color": [57, 230, 248, 127], "text_color": [25, 103, 116, 255]}', '{"boxes": [[0.2068696330991413, 0.6551418439716312], [0.9156908665105387, 0.6551418439716312], [0.9156908665105387, 0.6923758865248227], [0.2068696330991413, 0.6923758865248227]], "text": "다음은 리밸런싱 문제를 해결하기 위한 몇 가지 일반적인 접근 방법입니다", "confident": 0.9, "background_color": [56, 230, 248, 127], "text_color": [26, 102, 114, 255]}', '{"boxes": [[0.20843091334894615, 0.7207446808510638], [0.5175644028103045, 0.7207446808510638], [0.5175644028103045, 0.7562056737588653], [0.20843091334894615, 0.7562056737588653]], "text": "세션 및 하트비트 타임아웃 조정:", "confident": 0.9, "background_color": [57, 230, 248, 127], "text_color": [24, 103, 116, 255]}', '{"boxes": [[0.2068696330991413, 0.7828014184397163], [0.9859484777517564, 0.7828014184397163], [0.9859484777517564, 0.8200354609929078], [0.2068696330991413, 0.8200354609929078]], "text": "Kafka 컨슈머 구성에는 season.timeout.hearterval", "confident": 0.9, "background_color": [60, 231, 247, 127], "text_color": [57, 128, 121, 255]}', '{"boxes": [[0.2068696330991413, 0.8164893617021277], [0.9250585480093677, 0.8164893617021277], [0.9250585480093677, 0.8519503546099291], [0.2068696330991413, 0.8519503546099291]], "text": "session.t.ms는 컨슈머가 브로커에게 살아 있음을 알리", "confident": 0.9, "background_color": [59, 230, 247, 127], "text_color": [36, 111, 117, 255]}', '{"boxes": [[0.20608899297423888, 0.848404255319149], [0.9203747072599532, 0.848404255319149], [0.9203747072599532, 0.8847517730496454], [0.20608899297423888, 0.8847517730496454]], "text": "heartinterval.ms는 하트비트를 결정합니다.", "confident": 0.9, "background_color": [60, 231, 247, 127], "text_color": [42, 112, 115, 255]}', '{"boxes": [[0.20843091334894615, 0.8803191489361702], [0.9984387197501952, 0.8803191489361702], [0.9984387197501952, 0.9157801418439716], [0.20843091334894615, 0.9157801418439716]], "text": "이러한 값을 조정하여 컨슈머가 더 자주 하트비트를 전송하도록 하면", "confident": 0.9, "background_color": [57, 230, 247, 127], "text_color": [24, 98, 111, 255]}', '{"boxes": [[0.209992193598751, 0.9122340425531915], [0.4020296643247463, 0.9122340425531915], [0.4020296643247463, 0.9476950354609929], [0.209992193598751, 0.9476950354609929]], "text": "네트워크 문제 확인:", "confident": 0.9, "background_color": [57, 230, 248, 127], "text_color": [23, 102, 111, 255]}', '{"boxes": [[0.209992193598751, 0.9778368794326241], [0.297423887587822, 0.9778368794326241], [0.297423887587822, 1.0], [0.209992193598751, 1.0]], "text": "컨슈머아", "confident": 0.9, "background_color": [59, 229, 246, 127], "text_color": [27, 111, 124, 255]}'], ['ko', 'en', 'ja', 'de']))
    for message in consumer :
        try :
            processincomsumer(message.value)
        except KeyboardInterrupt as e:
            print (e)
            break
        except Exception as e:
            print (f"Error : {e}")
            print (traceback.format_exc())


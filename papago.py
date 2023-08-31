import json
import urllib.request
import urllib.error
import client_id_secret as sc 
import threading
import time
lock = threading.Lock()
# client_id_secret파일 .gitignore로 깃허브에 안올림 -> 네이버 API id, password
client_id = sc.client_id
client_secret = sc.client_secret
index = 0

threading_translated_result = []
def get_papago_response(url, id, secret, data):
    global index
    request = urllib.request.Request(url)
    for i in range(min(len (id), len(secret))):
        request.add_header("X-Naver-Client-Id", id[index])
        request.add_header("X-Naver-Client-Secret", secret[index])
        try:
            response = urllib.request.urlopen(request, data=data.encode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 429:
                index = (index + 1) % min(len (id), len(secret))
                print ('Too many request so change id, pw now', index)
                continue
            else:
                print(f"Error Code: {e.code}")
                return None
        else:
            rescode = response.getcode()
            if rescode == 200:
                return response
            else:
                print("Error Code:" + rescode)
                return None

def get_translate(text, target, index_in = None):
    # print(f"In get_translate -> type : {type(text)}, text : {text}")
    # print(f"In get_translate -> type : {type(target)}, target : {target}")
    global threading_translated_result
    print (text, target, index_in)

    lang = get_lang(text)
    #print ('lang', lang)
    #print(type(lang))
    if lang == 'unk': # 모르는 경우
        result = text
    elif lang == target: # 같은 경우
        result = text
    else: # 이외에 처리
        encText = urllib.parse.quote(text)
        data = f"source={str(lang)}&target={str(target)}&text=" + encText
        url = "https://openapi.naver.com/v1/papago/n2mt"
        response = get_papago_response(url, client_id, client_secret, data)
        if response is not None:
            response_body = response.read()
            response_body = json.loads(response_body.decode('utf-8'))
            result = response_body['message']['result']['translatedText']
        else:
            print ('response error default char')
            result = text    
            

        if index_in is None: # Not multi threading
            print ('multi_threading error')
            return result
        else: # multi threading
            print (index_in, result)
            
            threading_translated_result.append ((index_in, result))

        
def get_translate_list(text_list : list(), target):
    global threading_translated_result
    threading_translated_result = []
    threads = []
    for i, text in enumerate(text_list):
        t = threading.Thread(target=get_translate, args=(text, target, i))
        threads.append(t)
    
    for t in threads:
        #time.sleep(1)
        t.start()
        t.join()
    
    threading_translated_result = sorted(threading_translated_result, key = lambda x : x[0])
    print (threading_translated_result)
    return threading_translated_result

def get_lang(text):
    encQuery = urllib.parse.quote(text)
    data = "query=" + encQuery
    url = "https://openapi.naver.com/v1/papago/detectLangs"
    response = get_papago_response(url, client_id, client_secret, data)
    if response is not None:
        response_body = response.read()
        response_body =json.loads(response_body.decode('utf-8'))
        return response_body['langCode']
    else:
        return 'unk'


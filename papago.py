import json
import urllib.request
import urllib.error
import client_id_secret as sc 

# client_id_secret파일 .gitignore로 깃허브에 안올림 -> 네이버 API id, password
client_id = sc.client_id
client_secret = sc.client_secret
index = 0

def get_papago_response(url, id, secret, data):
    global index
    request = urllib.request.Request(url)
    request.add_header("X-Naver-Client-Id", id[index])
    request.add_header("X-Naver-Client-Secret", secret[index])
    try:
        response = urllib.request.urlopen(request, data=data.encode("utf-8"))
    except urllib.error.HTTPError as e:
        if e.code == 429:
            index = (index + 1) % min(len (id), len(secret))
            print ('Too many request so change id, pw now', index)
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
def get_translate(text, target):
    # print(f"In get_translate -> type : {type(text)}, text : {text}")
    # print(f"In get_translate -> type : {type(target)}, target : {target}")
    lang = get_lang(text)
    print ('lang', lang)
    print(type(lang))
    if lang == 'unk': # 모르는 경우
        return text
    elif lang == target: # 같은 경우
        return text
    else: # 이외에 처리
        encText = urllib.parse.quote(text)
        data = f"source={str(lang)}&target={str(target)}&text=" + encText
        url = "https://openapi.naver.com/v1/papago/n2mt"
        response = get_papago_response(url, client_id, client_secret, data)
        if response is not None:
            response_body = response.read()
            response_body = json.loads(response_body.decode('utf-8'))
            return response_body['message']['result']['translatedText']
        else:
            return text


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

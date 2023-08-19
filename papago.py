import json
import urllib.request
import client_id_secret as sc 

# client_id_secret파일 .gitignore로 깃허브에 안올림 -> 네이버 API id, password
client_id = sc.client_id
client_secret = sc.client_secret




def get_translate(text, target):
    # print(f"In get_translate -> type : {type(text)}, text : {text}")
    # print(f"In get_translate -> type : {type(target)}, target : {target}")
    lang = get_lang(text)
    #print(type(lang)) 같음
    if lang != target :
        encText = urllib.parse.quote(text)

        data = f"source={str(lang)}&target={str(target)}&text=" + encText
        url = "https://openapi.naver.com/v1/papago/n2mt"
        request = urllib.request.Request(url)
        request.add_header("X-Naver-Client-Id",client_id)
        request.add_header("X-Naver-Client-Secret",client_secret)
        response = urllib.request.urlopen(request, data=data.encode("utf-8"))
        rescode = response.getcode()
        if(rescode==200):
            response_body = response.read()
            response_body = json.loads(response_body.decode('utf-8'))
            return response_body['message']['result']['translatedText']
        else:
            print("Error Code:" + rescode)
    else :
        # print(f'same to same : {e}')
        return text

def get_lang(text):
    encQuery = urllib.parse.quote(text)
    data = "query=" + encQuery
    url = "https://openapi.naver.com/v1/papago/detectLangs"
    request = urllib.request.Request(url)
    request.add_header("X-Naver-Client-Id",client_id)
    request.add_header("X-Naver-Client-Secret",client_secret)
    response = urllib.request.urlopen(request, data=data.encode("utf-8"))
    rescode = response.getcode()
    if(rescode==200):
        response_body = response.read()
        response_body =json.loads(response_body.decode('utf-8'))
        return response_body['langCode']
    else:
        print("Error Code:" + rescode)


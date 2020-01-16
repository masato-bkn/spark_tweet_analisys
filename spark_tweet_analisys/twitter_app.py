import json
import socket
import sys
import traceback

import requests
import requests_oauthlib

# 認証キーの設定
CONSUMER_KEY    = "XXXXXXX"
CONSUMER_SECRET = "XXXXXXX"
ACCESS_TOKEN    = "XXXXXXX"
ACCESS_SECRET   = "XXXXXXX"
my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET)


def send_tweets_to_spark(http_resp, tcp_connection):
    '''
    ソケット経由でツイートをspark_app.pyに送信
    '''
    for line in http_resp.iter_lines():
        try:
            full_tweet = json.loads(line)            
            print("Tweet Text: " + full_tweet['text'])
            print ("------------------------------------------")
            text = full_tweet['text'].encode("utf-8")
            tcp_connection.send(text)
        except:
            e = sys.exc_info()[0]
            print("Error: %s" % e)
            traceback.print_exc()

def get_tweets():
    '''
    検索ワードを基にツイート取得
    '''
    try:
        url = 'https://stream.twitter.com/1.1/statuses/filter.json'
        query_data = [('track',sys.argv[1])]
        query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
        response = requests.get(query_url, auth=my_auth, stream=True)
        print(query_url, response)
        return response
    except Exception as e:
        print(e)

TCP_IP = "000.000.00.0"
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, 5555))
s.listen(2)
print("Waiting for TCP connection...")
conn, addr = s.accept()
print("Connected... Starting getting tweets.")

resp = get_tweets()
send_tweets_to_spark(resp,conn)

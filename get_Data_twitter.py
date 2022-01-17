import socket
import sys
import time
import requests
import json


#Socket info
PORT = 8499
IP = "localhost"

#Twitter API info
path = "https://api.twitter.com/2/tweets/search/recent?query=covid19%20lang%3Aen&max_results=100"
header = {"Authorization": "Bearer AAAAAAAAAAAAAAAAAAAAAPufXgEAAAAA98%2FCHZ54Ut9vSXKfzytDjM%2F5tDg%3Dt9dIfaImj9v2ybPnP3Czc6LoSjf5o62kyMl9MEI1hwbSme5rsF"}


def getingTweets():
    response = requests.get(path, headers=header)
    tweetsDetail = response.json()["data"][0]["text"]
    return tweetsDetail


conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((IP, PORT))
s.listen(1)
print("==> Start listening on ",PORT)
conn, addr = s.accept()
print("==> Connected successfully")
index = 1
while True:
    resp = getingTweets()
    try:
        conn.send(resp.encode())
        print(" ---------> Tweet N:",index,"<----------")
        index += 1
    except:
        print("-------------------> Error <--------------------")
        exit(1)
    time.sleep(5)
    
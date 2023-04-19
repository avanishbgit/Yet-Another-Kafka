import requests
import sys
from flask import Flask,request
import json
import os
from datetime import datetime


cport = 0 
cid = int(sys.argv[1][0])
base_path = "http://127.0.0.1:"
zookeeper_port = "8080"
def logger(cid, curr, topic):
    f = open('logs.txt', 'a')
    f.write(f'DateTime: {curr} : ConsumerID: {str(cid)} Topic Consumed From: {topic}\n')
    f.close()

def logger_sub(cid, curr, topic):
    f = open('logs.txt', 'a')
    f.write(f'DateTime: {curr} : ConsumerID: {str(cid)} Topic Subscribed To: {topic}\n')
    f.close()

def main():
    global cport
    app = Flask(__name__)
    
    cport = 8000 + int(sys.argv[1][0])
    type_consumer = int(sys.argv[2][0])
    subscriber_topic = input("Enter subscription topic: ")
    if type_consumer == 1:
        res = requests.get(base_path + zookeeper_port+"/find_leader")
        res = requests.post(base_path + res.text+f"/special_consumer/{subscriber_topic}", json = {'port': cport})
    flag = True
    while(flag):
        try:
            res = requests.get(base_path + zookeeper_port+"/find_leader")   # zookeeper
            res = requests.post(base_path + res.text+f"/subscribe_topic/{subscriber_topic}",json={"cport":cport}) #leader broker
            # print(res)
            now = datetime.now()
            logger_sub(cid, now, subscriber_topic)
            flag = False
        except Exception as e:
            print(e, "test")
            
    @app.route("/send_subscribed_posts/<topic_name>", methods=['POST'])
    def send_subscribed_posts(topic_name):
        outstring = str(request.data)[2:-1]
        outdict = json.loads(outstring)
        print(f"{outdict['pid']}({topic_name}) : {outdict['data']}")
        now = datetime.now()
        logger(cid, now, topic_name)
        return topic_name

    @app.route('/specialConsumer/<topic_name>', methods = ['POST'])
    def consume(topic_name):
        outstring = str(request.data)[2:-1]
        # cport = json.loads(outstring)
        print(outstring)
        return "Hey there! I'm using Whatsapp!"
    res = app.run(port = cport)
    
if __name__ == '__main__':
    try:
        main()
        res = requests.get(base_path + zookeeper_port+"/find_leader")   # zookeeper
        requests.get(f'http://127.0.0.1:{res.text}/deregister_consumer/{cport}')
        print("test")
    except KeyboardInterrupt:
        pass


       


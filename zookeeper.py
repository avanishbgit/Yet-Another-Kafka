from flask import Flask,Response, request
import os
import requests
import threading
import time
import random

app = Flask(__name__)
registered = []
alive = []
leader = None
POLLING_INTERVAL = 3

@app.route("/")
def home():
    return "home"

base_path = "http://127.0.0.1:"

@app.route("/find_leader")
def find():
    return str(leader)

def isalive(port):
    global leader
    flag = False
    while True:
        try:
            requests.get(f"http://127.0.0.1:{port}/polling",timeout=1)
            flag = True
            if port in alive:
                pass
            else:
                alive.append(port)
            
            # The first broker to start
            if leader is None:
                leader = port
                print("Leader------>", port)

            elif port != leader:
                print("Follower------>", port)
            else:
                print("Leader------>", port)
            
        except Exception as e:
            if flag:
                flag = False
                alive.remove(port)
                if leader == port and alive != []:
                    leader = random.choice(alive)
                    print("Leader changed to: ", leader)
                print(port,"stopped working")
            else:
                print(port, "yet to start")  
        time.sleep(POLLING_INTERVAL)

@app.route('/create_topic/<topic>')
def create_topic(topic):

    return topic
    # print('Recieved from zookeeper: {}'.format(request.data))
    # full_path = base_path + str(int())
    # res = requests.post(, json={f"{sys.argv[1][0]}":"send leader pls"})
    # if res.ok:
    #     print(res.json())
    # return Response('We recieved something…')

@app.route('/send_topic/<topic>', methods=['POST'])
def receive_topic(topic):
    print('Recieved from zookeeper: {}'.format(request.data))
    return topic

@app.route('/deregister_consumer/<port>')
def deregister(port):
    print(port, "has deregistered")
    return "Success"

def main():
    broker1 = threading.Thread(target=isalive, args=(9000,))
    broker1.daemon = True
    broker2 = threading.Thread(target=isalive, args=(9001,))
    broker2.daemon = True
    broker3 = threading.Thread(target=isalive, args=(9002,))
    broker3.daemon = True
    broker1.start()
    broker2.start()
    broker3.start()

    app.run(debug=False, use_reloader=False, port=8080)

if __name__ == "__main__":
    main()


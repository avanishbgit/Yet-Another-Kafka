import requests
import sys
from datetime import datetime


pid = int(sys.argv[1][0])
base_path = "http://127.0.0.1:"
zookeeper_port = "8080"

def logger(pid, curr, topic):
    f = open('logs.txt', 'a')
    f.write(f'DateTime: {curr} : ProducerID: {str(pid)} Topic Producing Topic: {topic}\n')
    f.close()

topic = input("Enter topic: ")
res = requests.get(base_path + zookeeper_port+"/find_leader") #zookeeper
res = requests.get(base_path+res.text+f"/create_topic/{topic}") #leader broker
while True:
    res = requests.get(base_path + zookeeper_port+"/find_leader") #zookeeper
    data = input("Enter data: ")
    flag = True
    while(flag):
        try:
            res = requests.post(base_path+res.text+f"/send_topic/{topic}", json={"pid":str(pid),"data":data},timeout = 2.4) #post to leader broker
            flag = False
            now = datetime.now()
            logger(pid, now, topic)
        except:
            pass
   
    


# if __name__ == '__main__':
#     app.run(debug=True)

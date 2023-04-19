from flask import Flask,Response, request
import os
import json
import requests
import shutil

app = Flask(__name__)
base_path = "http://127.0.0.1:"
zookeeper_port = "8080"
path = "b1"
consumer_path = "c"
@app.route("/")
def home():
    return "home"

@app.route("/create_topic/<topic_name>")
def create_topic(topic_name):
    if(not os.path.exists(path)):
        os.makedirs(path)
    if(not os.path.exists(path + "/"+ topic_name)):
        os.makedirs(path + "/" + topic_name)
    # files = os.listdir(path + "/" + topic_name)
    # print(files)
    # print(os.path.exists(path))
    return topic_name

@app.route("/send_topic/<topic_name>", methods=['POST'])
def send_topic(topic_name):
    outstring = str(request.data)[2:-1]
    outdict = json.loads(outstring)
    print(outstring)
    # files = os.listdir(path + "/" + topic_name)
    # print(files)
    # print(os.path.exists(path))
    thresh = 5
    filen = 'partition1'
  
    f1 = open(path + "/"+ topic_name+"/"+"partition1.txt", 'a+')
    f1.seek(0)
    abc = f1.readlines()
    # print(len(abc))
    if len(abc) >= thresh:
        f2 = open(path + "/"+ topic_name+"/"+"partition2.txt", 'a+')
        f2.seek(0)
        if len(f2.readlines()) >= thresh:
            filen = 'partition3'
        else:
            filen = 'partition2'
        
        f2.close()
    f1.close() 
    fn = open(f"{path}/{topic_name}/{filen}.txt", 'a')
    fn.write(outdict["data"]+"\n")
    fn.close()

    with open(consumer_path+"/"+'consumer.json', 'r') as openfile:
        mydict = json.load(openfile)
    ports = mydict[topic_name]
    for port in ports:
        res = requests.post(base_path + str(port)+f"/send_subscribed_posts/{topic_name}",json=outdict) #leader broker
    
    return topic_name

@app.route('/special_consumer/<topic_name>', methods = ['POST'])
def get_files(topic_name):
    outstring = str(request.data)[2:-1]
    cport = json.loads(outstring)
    filenames = os.listdir(path+"/"+topic_name)
    print(filenames)
    for file in filenames:
        f1 = open(path+"/"+topic_name+"/"+file, 'r')
        data = f1.read()
        f1.close()
        print(data)
        print('==============')
        # print(base_path + str(cport['port'])+f"/specialConsumer/{topic_name}")
        # requests.post(base_path + str(cport['port'])+f"/specialConsumer/{topic_name}", json = {'data' : data})



@app.route('/subscribe_topic/<topic_name>', methods=['POST'])
def subscribe_topic(topic_name):
    outstring = str(request.data)[2:-1]
    cport = json.loads(outstring)
    print(f'Recieved from zookeeper: {outstring}')
    if(not os.path.exists(consumer_path)):
        os.makedirs(consumer_path)
        mydict = {topic_name : [cport['cport']]}
        with open(consumer_path+"/"+"consumer.json", "w") as outfile:
            json.dump(mydict, outfile)
    else:
        with open(consumer_path+"/"+'consumer.json', 'r') as openfile:
            mydict = json.load(openfile)
        with open(consumer_path+"/"+'consumer.json', 'w') as outfile:
            if(topic_name not in mydict.keys()):
                mydict[topic_name] = [cport['cport']]
            else:
                if cport['cport'] not in mydict[topic_name]:
                    mydict[topic_name].append(cport['cport'])
            json.dump(mydict, outfile)
    return topic_name
        
@app.route('/deregister_consumer/<port>', methods=['POST','GET'])
def deregister(port):
    mydict = dict()
    with open(consumer_path+"/"+'consumer.json', 'r') as openfile:
        mydict = json.load(openfile)
    for i in mydict:
        if int(port) in mydict[i]:
            mydict[i].remove(int(port))
            print(mydict, mydict[i])
    with open(consumer_path+"/"+'consumer.json', 'w') as openfile:
        openfile.write(json.dumps(mydict))

    print(port, "has deregistered")
    return "Success"

@app.route('/polling')
def polling():
    shutil.copy
    return "Hey there! I'm using Whatsapp!"
if __name__ == "__main__":
    app.run(port = 9000)
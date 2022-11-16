import requests, base64
from requests.auth import HTTPBasicAuth 
from datetime import datetime
from collections import Counter


start_time = datetime.now() 
print("Start => {0}").format(start_time)

url_base = "https://xpto-80ac44.es.us-east-1.aws.found.io:9243"
username = "juser"
password = "password"

usrPass = username+":"+password
b64Val = base64.b64encode(usrPass)

url = url_base+"/_cat/nodeattrs?v&h=node,value"
    
headers = {
    'Content-Type': 'application/json',
    "Authorization": "Basic %s" % b64Val
}

response = requests.request("GET", url, headers=headers)

if "error" in response.text:
    print("======> ERROR! <======")
else:
    nodes_attr = response.text

nodes_attr = nodes_attr.split("\n")

nodes_hot = []

for control in nodes_attr:
    control = control.split(" ")
    try:
        node = control[0]
        attr = control[1]
        if attr == 'hot':
            nodes_hot.append(node)
    except:
        pass


url = url_base+"/_cat/shards?v&h=index,node"
    
headers = {
    'Content-Type': 'application/json',
    "Authorization": "Basic %s" % b64Val
}

response = requests.request("GET", url, headers=headers)

if "error" in response.text:
    print("======> ERROR! <======")
else:
    shards_attr = response.text

shards_attr = shards_attr.split("\n")

indices_hot = []

for control_shard in shards_attr:
    control_shard = control_shard.split(" ")
    try:
        exist_hot = nodes_hot.count(control_shard[-1])
        if exist_hot > 0:
            indices_hot.append(control_shard[0])
    except:
        pass

def unique(list1):
    unique_list = []
    for x in list1:
        if x not in unique_list:
            unique_list.append(x)

    return unique_list

print("### INDICES EM HOT ###")
total_hot = len(unique(indices_hot))
print("[*] Total: %i" % total_hot)

now = datetime.now()
print("\n\nStart => {0}").format(start_time) 
print("End => {0}").format(now)
print("Execution time => {0}").format(now - start_time)
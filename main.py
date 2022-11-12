import requests, base64
from requests.auth import HTTPBasicAuth 
from datetime import datetime

start_time = datetime.now() 
print("Start => {0}").format(start_time)

# INFORME O ENDPOINT DO ELASTICSEARCH
url_base = "https://endpoint.es.us-east-1.aws.found.io:9243"
# INFORME O USUARIO
username = "foo"
# INFORME A SENHA
password = "bar"

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
nodes_warm = []
nodes_cold = []
nodes_frozen = []

for control in nodes_attr:
    control = control.split(" ")
    try:
        node = control[0]
        attr = control[1]
        if attr == 'hot':
            nodes_hot.append(node)
        if attr == 'warm':
            nodes_warm.append(node)
        if attr == 'cold':
            nodes_cold.append(node)
        if attr == 'frozen':
            nodes_frozen.append(node)
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
indices_warm = []
indices_cold = []
indices_frozen = []

for control_shard in shards_attr:
    control_shard = control_shard.split(" ")
    try:
        exist_hot = nodes_hot.count(control_shard[-1])
        exist_warm = nodes_warm.count(control_shard[-1])
        exist_cold = nodes_cold.count(control_shard[-1])
        exist_frozen = nodes_frozen.count(control_shard[-1])
    
        # checking if it is more than 0
        if exist_hot > 0:
            indices_hot.append(control_shard[0])
        if exist_warm > 0:
            indices_warm.append(control_shard[0])
        if exist_cold > 0:
            indices_cold.append(control_shard[0])
        if exist_frozen > 0:
            indices_frozen.append(control_shard[0])
    except:
        pass


print("### INDICES EM HOT ###")
print("[*] Total: %i" % len(indices_hot))
for hot in indices_hot:
    print(hot)

print("\n\n### INDICES EM WARM ###")
print("[*] Total: %i" % len(indices_warm))
for warm in indices_warm:
    print(warm)

print("\n\n### INDICES EM COLD ###")
print("[*] Total: %i" % len(indices_cold))
for cold in indices_cold:
    print(cold)

print("\n\n### INDICES EM FROZEN ###")
print("[*] Total: %i" % len(indices_frozen))
for frozen in indices_frozen:
    print(frozen)

now = datetime.now()
print("\n\nStart => {0}").format(start_time) 
print("End => {0}").format(now)
print("Execution time => {0}").format(now - start_time)
import json

file = open('smallTwitter.json')

data = json.load(file)

for i in data['rows']:
    print(i)

file.close()
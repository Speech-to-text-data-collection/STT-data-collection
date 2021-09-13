from flask import Flask, render_template, request
from redis import Redis
import os, sys
from kafka import KafkaConsumer
import json

local_boostrap_server_address = 'localhost:9093'
sys.path.append(os.path.abspath(os.path.join('../application')))

app = Flask(__name__)
redis = Redis(host='redis', port=6379)
a = 0
try:
    consumer = KafkaConsumer(
                    "topic0001",
                    bootstrap_servers=local_boostrap_server_address,
                    auto_offset_reset='latest',
                    group_id="consumer-group-a")
    a=1

except:
    pass

@app.route('/', methods=['GET', 'POST'])
def home():
    if request.method == "GET":
        e = ""
        if (a==1):
            try:
                for msg in consumer:
                    print("Registered User = {}".format(json.loads(msg.value)))
                    data_received = json.loads(msg.value)
                    break
            except Exception as e:
                print(e)
                data_received = "Nothing received"
            # redis.incr('hits') count=redis.get('hits'),
        
        else:
            try:
                consumer = KafkaConsumer(
                                "topic0001",
                                bootstrap_servers=local_boostrap_server_address,
                                auto_offset_reset='latest',
                                group_id="consumer-group-a")
                for msg in consumer:
                    print("Registered User = {}".format(json.loads(msg.value)))
                    data_received = json.loads(msg.value)
                    break

            except:
                pass

        
        return render_template("index.html", 
                                count=5, 
                                text_corpus=data_received,
                                debg="e")

    if request.method == "POST":
        data = request.form['dummy_data']
        data_dummy = "Data you sent: > " +str(data)
        try:
            for msg in consumer:
                print("Registered User = {}".format(json.loads(msg.value)))
                data_received = json.loads(msg.value)

        except Exception as e:
            print(e)
            data_received = "Nothing received"
        # redis.incr('hits') count=redis.get('hits'),
        return render_template("index.html", count=5, 
                                text_corpus=data_received, 
                                data_sent = data_dummy)


if __name__ == "__main__":
    app.run(host="0.0.0.0",debug=True)
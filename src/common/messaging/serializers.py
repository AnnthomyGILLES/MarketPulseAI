# serializers.py
import json


def serialize_message(data):
    return json.dumps(data).encode('utf-8')


def deserialize_message(message):
    return json.loads(message.decode('utf-8'))

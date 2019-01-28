import json

from redis import StrictRedis
from flask import request
from flask import Flask

from lib.event_store import EventStore


app = Flask(__name__)
redis = StrictRedis(decode_responses=True, host='redis')
store = EventStore(redis)


@app.route('/email', methods=['POST'])
def post():

    values = json.loads(request.data)
    if not values['to'] or not values['msg']:
        raise ValueError("missing mandatory parameter 'to' and/or 'msg'")

    #app.logger.info('sent email with message "{}" to "{}"'.format(values['msg'], values['to']))
    return json.dumps({"result": True})

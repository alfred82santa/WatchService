#!/usr/bin/python
#
#
# Copyright (C) Alfred 2012 <alfred82santa@gmail.com>
# 
# safeTicketApp is free software: you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by the
# Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# safeTicketApp is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License along
# with this program.  If not, see <http://www.gnu.org/licenses/>.

import json
from functools import wraps
from flask import Flask, request, Response
from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime, timedelta


APP_PORT = 8080
APP_HOST = '0.0.0.0'

MONGO_HOST = 'localhost'
MONGO_PORT = 27017

MONGO_DB = 'watcher'
MONGO_COLLECTION = 'notification'

VALID_NAMESPACE = ["tdaf",]
VALID_STATUS = ["ACTIVE", "PAUSED",]

app = Flask(__name__)

def get_collection(mongo=MongoClient(MONGO_HOST, MONGO_PORT)):
    return mongo[MONGO_DB][MONGO_COLLECTION]
    
def json_output(func):
    @wraps(func)
    def decorator(*args, **kwargs):
        return Response(json.dumps(func(*args, **kwargs)), mimetype="applicaton/json")
    return decorator

@app.route('/watcher', methods=['POST'])
@json_output
def create_watcher():
    watcher = json.loads(request.data)
    watcher = validate_watcher(watcher)
    wid = str(get_collection().insert(watcher))
    
    return {'id': wid}
    
def validate_watcher(watcher):
    data = {}
    if 'id' in watcher.viewkeys():
        raise Exception("id field is read-only")
    if 'scopeId' not in watcher.viewkeys():
        raise Exception("scopeId field is required")
    data['scopeId'] = watcher['scopeId']
    if 'namespace' not in watcher.viewkeys():
        raise Exception("namespace field is required")
    data['namespace'] = watcher['namespace']
    if 'entityType' not in watcher.viewkeys():
        raise Exception("entityType field is required")
    data['status'] = watcher.get('status', 'ACTIVE') 
    if data['status'] not in VALID_STATUS:
        raise Exception("status must be ACTIVE or PAUSED") 
    data['entityType'] = watcher['entityType']
    if 'entityIds' not in watcher.viewkeys():
        raise Exception("entityIds field is required")
    if  not isinstance(watcher['entityIds'], list):
        raise Exception("entityIds must be a string list")
    data['entityIds'] = watcher['entityIds']
    if 'notificationCount' in watcher.viewkeys():
        if not isinstance(watcher['notificationCount'], (int, long)):
            raise Exception("notificationCount must be an integer")
        if watcher['notificationCount'] < 1:
            raise Exception("notificationCount must be greater then 0")
        data['notificationCount'] = watcher['notificationCount']
    if 'expire' not in watcher.viewkeys():
        raise Exception("expire field is required")
    if not isinstance(watcher['expire'], (int, long)):
        raise Exception("expire must be an integer")
    data['expire'] = datetime.utcnow() + timedelta(seconds=watcher['expire'])
    data['created'] = datetime.utcnow()
    return data
    
@app.route('/watcher/<wid>', methods=['GET'])
@json_output
def get_watcher(wid):
    item = get_collection().find_one({'_id':ObjectId(wid)})
    return map_watcher(item)
    
@app.route('/watcher/<wid>/status', methods=['PUT'])
@json_output
def set_watcher_status(wid):
    data = json.loads(request.data)
    get_collection().update({'_id': ObjectId(wid)}, {'$set': {'status' : request.data}})
    return {'success': True}
    
@app.route('/watcher/<wid>', methods=['DELETE'])
@json_output
def remove_watcher(wid):
    get_collection().remove({'_id': ObjectId(wid)})
    return {'success': True}
    
@app.route('/watcher', methods=['GET'])
@json_output
def list_watchers():
    result = []
    for item in get_collection().find():
        result.append(map_watcher(item))
    return result
    
def map_watcher(item):
    item['id'] = str(item['_id'])
    del item['_id']
    item['created'] = str(item['created'])
    item['expire'] = str(item['expire'])
    return item

if __name__ == '__main__':
    app.debug = True
    app.run(host=APP_HOST, port=APP_PORT)


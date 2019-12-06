from pymongo import MongoClient
from bson import json_util

from datetime import datetime, timedelta
import json

from configuration import *


mongo_client = MongoClient(host=MONGO_URL, port=MONGO_PORT, username=MONGO_USER, password=MONGO_PASS, document_class=dict)
db = mongo_client.rsvps

def get_all_groups():
    groups = db.groups.find({})
    return groups


def get_closest_n_groups(lon, lat, n):
    query = {
        "location": {
            "$near": {
                "$geometry": {
                    "type": "Point",
                    "coordinates": [float(lon), float(lat)]
                }
                
            }
        }
    }
    groups = db.groups.find(query).limit(n)
    return list(groups)

def get_top_cities(date, n):
    target_date = datetime.fromisoformat(date)
    query = {
        "date": {
            "$gte" : target_date,
            "$lt": target_date + timedelta(days=1)
        }
    }
    projection = {
        "city": 1,
        "date": 1,
        "_id": 0
    }
    aggregation_pipeline = [
        {"$match": query},
        {"$group": {
                "_id": {
                    "city": "$city"
                },
                "count": {
                    "$sum": "$rsvps"
                },
                }
            },
        {"$sort": {"count": -1}},
        {"$limit": n}
    ]
    result = db.attendances.aggregate(aggregation_pipeline)

    return json.loads(json_util.dumps(result))

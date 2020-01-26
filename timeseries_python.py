#!/usr/bin/env python3

# python3 timeseries_python.py 1580029339 1580029399 1 "key:field"

import sys
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
import json
import pprint
from datetime import datetime

# User Data
start = sys.argv[1]
end = sys.argv[2]
sample = str(sys.argv[3])
interval = str(int((int(end) - int(start))/int(sample))) + 's'
metric_name = sys.argv[4]

q_start_time = str(datetime.utcfromtimestamp(int(start)).isoformat())+'.000Z'
q_end_time = str(datetime.utcfromtimestamp(int(end)).isoformat())+'.000Z'

histogram_start_time = int(sys.argv[1])*1000
histogram_end_time = int(sys.argv[2])*1000

query = {
    "bool": {
        "must": [
            {
                "range": {
                    "@timestamp": {
                        "gte": q_start_time,
                        "lt": q_end_time,
                        "format": "strict_date_optional_time"
                    }
                }
            }
        ]
    }
}

filters = {
    "filters": {
        metric_name: {
            "query_string": {
                "query": metric_name
            }
        }
    }
} 

date_histogram = {
    "field": "@timestamp",
    "interval": interval,
    "extended_bounds": {
        "min": histogram_start_time,
        "max": histogram_end_time
    },
    "min_doc_count": 0
}

aggs = {
    "avg(mean)": {
        "avg": {
            "field": "mean"
        }
    }
}

body = {
        "query": query,
        "aggs": {
            "q": {
                "meta": {
                    "type": "split"
                },
                "filters": filters,
                "aggs": {
                    "time_buckets": {
                        "meta": {
                            "type": "time_buckets"
                        },
                        "date_histogram": date_histogram,
                        "aggs": aggs
                    }
                }
            }
        },
        "size": 0
    }

final_query = {
    "index": "statsd_timerdata-*",
    "body":body
}

client = Elasticsearch()
response = client.search(
    index = final_query["index"],
    body = final_query["body"]
)

response = response["aggregations"]["q"]["buckets"][metric_name]

for point in response["time_buckets"]["buckets"]:
    print(interval, histogram_start_time, histogram_end_time)
    print(q_start_time, q_end_time)
    print(json.dumps(point, indent=4, sort_keys=True))

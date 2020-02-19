#!/usr/bin/env bash
curl -X PUT "localhost:9200/test-index?pretty"
curl -X PUT "localhost:9200/test-index/_settings?pretty" -H 'Content-Type: application/json' -d'
{
    "index" : {
        "refresh_interval" : "-1"
    }
}
'
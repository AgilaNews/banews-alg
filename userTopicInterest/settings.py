# -*- coding: utf-8 -*-

import socket

# mysql, redis & es configuration of local, sandbox & online environment
HOSTNAME = socket.gethostname()
RD_HOSTNAME_LST = ['banews', ]
SANDBOX_HOSTNAME_LST = ['sandbox-1', 'hadoop-1']
ONLINE_HOSTNAME_LST = ['spider-1']
if HOSTNAME in RD_HOSTNAME_LST:
    CURRENT_ENVIRONMENT_TAG = 'local'
elif HOSTNAME in SANDBOX_HOSTNAME_LST:
    CURRENT_ENVIRONMENT_TAG = 'sandbox'
elif HOSTNAME in ONLINE_HOSTNAME_LST:
    CURRENT_ENVIRONMENT_TAG = 'online'
else:
    CURRENT_ENVIRONMENT_TAG = None
    print "Spider doesn't deploy on local, sandbox or online machine, Error!!!"
    exit(0)
ENVIRONMENT_CONFIG = {
    "local": {
        "mysql_config": {
            "host": "127.0.0.1",
            "port": 3306,
            "user": "root",
            "passwd": "MhxzKhl-Happy!@#",
            "database": "banews"
        },
        "scrapy_redis_config": {
            "host": "127.0.0.1",
            "port": 6379
        },
        "news_queue_redis_config": {
            "host": "127.0.0.1",
            "port": 6379
        },
        "redis_crawler_keys": "newsCrawler_item_key",
        "elastic_search_config": {
            # protocol://username:passwd@host:port
            "servers": ["http://localhost:9200", ],
            "index": "banews-article",
            "type": "article",
            "buff_length": 500,
            "timeout": 60,
        }
    },
    "online": {
        "mysql_config": {
            "host": "10.8.22.123",
            "port": 3306,
            "user": "banews_w",
            "passwd": "MhxzKhl-Happy",
            "database": "banews"
        },
        "scrapy_redis_config": {
            "host": "127.0.0.1",
            "port": 6379
        },
        "news_queue_redis_config": {
            "host": "10.8.7.6",
            "port": 6379
        },
        "redis_crawler_keys": "newsCrawler_item_key",
        "elastic_search_config": {
            "servers": ["http://10.8.18.130:9200", ],
            "index": "banews-article",
            "type": "article",
            "buff_length": 500,
            "timeout": 60,
        }
    },
    "sandbox": {
        "mysql_config": {
            "host": "10.8.6.7",
            "port": 3306,
            "user": "banews_w",
            "passwd": "MhxzKhl-Happy",
            "database": "banews"
        },
        "scrapy_redis_config": {
            "host": "10.8.6.7",
            "port": 6379
        },
        "news_queue_redis_config": {
            "host": "10.8.14.136",
            "port": 6379
        },
        "redis_crawler_keys": "newsCrawler_item_key",
        "elastic_search_config": {
            "servers": ["http://localhost:9200", ],
            "index": "banews-article",
            "type": "article",
            "buff_length": 500,
            "timeout": 60,
        }
    }
}

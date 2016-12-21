#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
# @Date    : 2016-12-20 15:44:26
# @Author  : Zhao Yulong (elysium.zyl@gmail.com)
# @Link    : ${link}
# @Describe: 
"""

import os
import traceback
from redis import Redis
import MySQLdb
import time
import math

class VideoGroup(object):
    """docstring for Video"""
    def __init__(self, redis, mysql):
        super(VideoGroup, self).__init__()
        self.redis_client = redis
        self.mysql_client = mysql

    def score(self):
        populars = self.redis_client.zrange("banews:ph:30001", 0, -1)
        for news_id in populars:
            showInfo = self.getShowInfo(news_id)
            newsInfo = self.getPublishTime(news_id)
            score = self.calc(showInfo, newsInfo)
            self.redis_client.zadd("banews:ph:30001", news_id, score)

    def getShowInfo(self, news_id):
        sql = '''select click_num, show_num from `banews-report`.`tb_video_stat` where url_sign = '%s' ''' % news_id
        cursor = self.mysql_client.cursor()
        cursor.execute(sql)
        result = cursor.fetchone()
        return result

    def getPublishTime(self, news_id):
        sql = '''select fetch_time,liked from `banews`.`tb_news` where url_sign = '%s' ''' % news_id
        cursor = self.mysql_client.cursor()
        cursor.execute(sql)
        result = cursor.fetchone()
        return result

    def calc(self, showInfo, newsInfo):
        try:
            click_num = showInfo[0]
            show_num = showInfo[1]
        except:
            click_num = 0
            show_num = 0
        try:
            fetch_time = newsInfo[0]
            liked = newsInfo[1]
        except:
            return 0
        if show_num == 0:
            click_rate = 0
        else:
            click_rate = math.log(1.0 * click_num / show_num + 1)

        if click_num == 0:
            like_rate = 0
        else:
            like_rate = 1.0 * liked / click_num 

        score = 600 * math.exp(-1.0 * ((time.time() - float(fetch_time)) / (86400*3))) + \
                5000 * click_rate + \
                100 * like_rate + \
                300 * math.exp(-1.0 * show_num/3000)
        return score
        

def main():
    redis_online = Redis("10.8.15.189", port=6379)
    redis_sandbox = Redis("10.8.14.136", port=6379)
    mysql_online = MySQLdb.connect("10.8.22.123", "banews_w", "MhxzKhl-Happy", "banews")

    video_group = VideoGroup(redis_sandbox, mysql_online)
    video_group = VideoGroup(redis_online, mysql_online)
    video_group.score()


if __name__ == '__main__':
    main()


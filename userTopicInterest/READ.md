+ 策略生成变量
    + ALG_TOPIC_NEWS_SCO_KEY: 获取近3天内发布的新闻，并根据近3天内用户的阅读量、点赞 & 评论量综合计算的分值
    + ALG_TOPIC_RATIO_KEY: 各个topic在用户近3天内，点击的整体分布. (对应paper中的p^0(category=c_i))
    + ALG_USER_TOPIC_KEY: 用户在近60天内，阅读新闻转化成对应topic上兴趣的分布
    + ALG_TOPIC_NEWS_QUEUE_: 线上入库文章经过模型预测后，隶属于各个topic的新闻队列 

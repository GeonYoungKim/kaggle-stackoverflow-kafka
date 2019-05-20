package com.skuniv.cs.geonyeong.kaggle.service;

import com.skuniv.cs.geonyeong.kaggle.enums.KafkaTopicType;

public interface Processor {
    void start();
    KafkaTopicType getKafkaTopicType();
}

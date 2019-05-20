package com.skuniv.cs.geonyeong.kaggle.manager;

import avro.shaded.com.google.common.collect.ImmutableMap;
import com.skuniv.cs.geonyeong.kaggle.enums.KafkaTopicType;
import com.skuniv.cs.geonyeong.kaggle.service.Processor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

@Slf4j
public class ProcessorManager {
    private ImmutableMap<String, Processor> processorImmutableMap;

    public ProcessorManager(Map<String, Processor> processorMap) {
        this.processorImmutableMap = ImmutableMap.copyOf(processorMap);
    }

    public void startConsumer(List<KafkaTopicType> kafkaTopicTypeList) {
        this.processorImmutableMap.values().forEach(item -> {
            if(kafkaTopicTypeList.contains(item.getKafkaTopicType()))
                item.start();
        });
    }
}

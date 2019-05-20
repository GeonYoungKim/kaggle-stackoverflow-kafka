package com.skuniv.cs.geonyeong.kaggle;

import com.skuniv.cs.geonyeong.kaggle.enums.KafkaTopicType;
import com.skuniv.cs.geonyeong.kaggle.manager.ProcessorManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
@SpringBootApplication
public class KaggleKafkaApplication implements ApplicationRunner {
    @Autowired
    private ProcessorManager processorManager;

    public static void main(String[] args) {
        SpringApplication.run(KaggleKafkaApplication.class, args);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        List<String> topicList = args.getOptionValues("topic");
        if (CollectionUtils.isNotEmpty(topicList)) {
            List<KafkaTopicType> kafkaTopicTypeList = topicList.stream()
                    .filter(item -> Optional.ofNullable(KafkaTopicType.valueOf(item)).isPresent())
                    .map(item -> KafkaTopicType.valueOf(item))
                    .collect(Collectors.toList());
            processorManager.startConsumer(kafkaTopicTypeList);
        }
    }
}

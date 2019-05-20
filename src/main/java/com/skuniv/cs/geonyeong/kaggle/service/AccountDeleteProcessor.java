package com.skuniv.cs.geonyeong.kaggle.service;

import com.skuniv.cs.geonyeong.kaggle.enums.KafkaTopicType;
import com.skuniv.cs.geonyeong.kaggle.utils.KafkaConsumerFactoryUtil;
import com.skuniv.cs.geonyeong.kaggle.vo.avro.AvroAccount;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Arrays;

import static com.skuniv.cs.geonyeong.kaggle.constant.KafkaConsumerConstant.CONSUME_WAIT_TIME;
import static com.skuniv.cs.geonyeong.kaggle.constant.KafkaConsumerConstant.POLL_SECOND;

@Slf4j
@Component
@RequiredArgsConstructor
public class AccountDeleteProcessor implements InitializingBean, DisposableBean, Processor {
    private final EsClient esClient;
    private KafkaConsumer<String, AvroAccount> consumer;

    @Override
    public void afterPropertiesSet() throws Exception {
        consumer = KafkaConsumerFactoryUtil.createKafkaConsumer(Arrays.asList(new KafkaTopicType[]{KafkaTopicType.ACCOUNT_DELETE}));
        log.info("AccountDeleteProcessConfiguration");
    }

    @Override
    public void destroy() {
        consumer.close();
    }

    @Override
    public void start() {
//        while (true) {
//            ConsumerRecords<String, AvroAccount> records = consumer.poll(Duration.ofSeconds(POLL_SECOND));
//            if (records.isEmpty()) {
//                try {
//                    Thread.sleep(CONSUME_WAIT_TIME);
//                } catch (InterruptedException e) {
//                    log.error("InterruptedException => {}", e);
//                }
//            }
//
//            records.forEach(record -> {
//                // TODO : es account 삭제 처리.
//            });
//        }
        log.info("AccountDeleteProcessor");
    }

    @Override
    public KafkaTopicType getKafkaTopicType() {
        return KafkaTopicType.ACCOUNT_DELETE;
    }
}

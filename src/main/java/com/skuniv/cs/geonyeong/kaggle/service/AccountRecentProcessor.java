package com.skuniv.cs.geonyeong.kaggle.service;

import static com.skuniv.cs.geonyeong.kaggle.constant.KafkaConsumerConstant.CONSUME_WAIT_TIME;
import static com.skuniv.cs.geonyeong.kaggle.constant.KafkaConsumerConstant.POLL_SECOND;

import com.skuniv.cs.geonyeong.kaggle.dao.AccountDao;
import com.skuniv.cs.geonyeong.kaggle.enums.KafkaTopicType;
import com.skuniv.cs.geonyeong.kaggle.utils.KafkaConsumerFactoryUtil;
import com.skuniv.cs.geonyeong.kaggle.vo.avro.AvroAccount;
import java.time.Duration;
import java.util.Arrays;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class AccountRecentProcessor implements InitializingBean, DisposableBean, Processor {

    private final AccountDao accountDao;

    @Value("${com.skuniv.cs.geonyeong.kaggle.es.index.post}")
    private String esPostIndex;

    @Value("${com.skuniv.cs.geonyeong.kaggle.es.index.account}")
    private String esAccountIndex;

    @Value("${com.skuniv.cs.geonyeong.kaggle.es.type}")
    private String esType;

    private KafkaConsumer<String, AvroAccount> consumer;

    @Override
    public void afterPropertiesSet() throws Exception {
        consumer = KafkaConsumerFactoryUtil
            .createKafkaConsumer(
                Arrays.asList(new KafkaTopicType[]{KafkaTopicType.ACCOUNT_RECENT}));
        log.info("AccountRecentProcessConfiguration");
    }

    @Override
    public void destroy() {
        consumer.close();
    }

    @Override
    public void start() {
        // account index upsert 및 post index는 update 처리.
        log.info("AccountRecentProcessor");
        while (true) {
            ConsumerRecords<String, AvroAccount> records = consumer
                .poll(Duration.ofSeconds(POLL_SECOND));
            if (records.isEmpty()) {
                try {
                    Thread.sleep(CONSUME_WAIT_TIME);
                } catch (InterruptedException e) {
                    log.error("InterruptedException => {}", e);
                }
                continue;
            }
            accountDao.upsertAccount(records);
        }
    }

    @Override
    public KafkaTopicType getKafkaTopicType() {
        return KafkaTopicType.ACCOUNT_RECENT;
    }
}


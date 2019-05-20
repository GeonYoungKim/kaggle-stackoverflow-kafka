package com.skuniv.cs.geonyeong.kaggle.utils;

import com.skuniv.cs.geonyeong.kaggle.enums.KafkaTopicType;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class KafkaConsumerFactoryUtil {

    private static Properties consumerProperties() throws ConfigurationException {
        String bootstrapServers = YmlUtil.getYmlProps().getProperty("com.skuniv.cs.geonyeong.kaggle.kafka.bootstrapServers");
        String schemaRegistryUrl = YmlUtil.getYmlProps().getProperty("com.skuniv.cs.geonyeong.kaggle.schema.registry.url");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaConsumerFactoryUtil.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        return props;
    }

    public static <T> KafkaConsumer<String, T> createKafkaConsumer(List<KafkaTopicType> topics) {
        KafkaConsumer<String, T> kafkaConsumer = null;
        try {
            kafkaConsumer = new KafkaConsumer<String, T>(consumerProperties());
        } catch (ConfigurationException e) {
            throw new RuntimeException("ConfigurationException => {}", e);
        }
        List<String> topicList = topics.stream().map(KafkaTopicType::name).collect(Collectors.toList());
        kafkaConsumer.subscribe(topicList);
        return kafkaConsumer;
    }
}

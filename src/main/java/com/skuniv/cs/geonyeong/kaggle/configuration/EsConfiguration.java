package com.skuniv.cs.geonyeong.kaggle.configuration;

import com.skuniv.cs.geonyeong.kaggle.service.EsClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class EsConfiguration {

    @Value("${com.skuniv.cs.geonyeong.kaggle.es.host}")
    private String host;

    @Value("${com.skuniv.cs.geonyeong.kaggle.es.port}")
    private int port;

    @Bean
    public EsClient esClient() {
        log.info("host => {}", host);
        log.info("port => {}", port);
        return new EsClient(new RestHighLevelClient(RestClient.builder(new HttpHost(host, port))));
    }
}

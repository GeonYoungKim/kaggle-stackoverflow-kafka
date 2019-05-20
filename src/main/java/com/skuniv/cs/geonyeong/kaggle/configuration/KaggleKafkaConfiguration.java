package com.skuniv.cs.geonyeong.kaggle.configuration;

import com.skuniv.cs.geonyeong.kaggle.manager.ProcessorManager;
import com.skuniv.cs.geonyeong.kaggle.service.Processor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KaggleKafkaConfiguration {

    @Autowired(required = false)
    public Map<String, Processor> processorMap = new HashMap<>();

    @Bean
    public ProcessorManager processorManager() {
        return new ProcessorManager(processorMap);
    }
}

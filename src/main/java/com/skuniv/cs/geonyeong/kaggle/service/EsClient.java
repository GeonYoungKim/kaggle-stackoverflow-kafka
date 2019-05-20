package com.skuniv.cs.geonyeong.kaggle.service;

import com.google.gson.Gson;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.RestHighLevelClient;

@Slf4j
@RequiredArgsConstructor
public class EsClient {
    private static final Gson gson = new Gson();
    private final RestHighLevelClient restHighLevelClient;
}


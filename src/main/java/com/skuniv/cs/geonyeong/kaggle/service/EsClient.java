package com.skuniv.cs.geonyeong.kaggle.service;

import com.google.gson.Gson;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;

@Slf4j
@RequiredArgsConstructor
public class EsClient {
    private static final Gson gson = new Gson();
    private final RestHighLevelClient restHighLevelClient;

    public BulkResponse bulk(BulkRequest bulkRequest) {
        BulkResponse bulkItemResponses = null;
        try {
            bulkItemResponses = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return bulkItemResponses;
    }

    public BulkByScrollResponse update(UpdateByQueryRequest updateByQueryRequest) {
        BulkByScrollResponse bulkByScrollResponse;
        try {
            bulkByScrollResponse = restHighLevelClient.updateByQuery(updateByQueryRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return bulkByScrollResponse;
    }
}


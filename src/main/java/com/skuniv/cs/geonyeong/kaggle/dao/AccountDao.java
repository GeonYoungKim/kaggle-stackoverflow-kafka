package com.skuniv.cs.geonyeong.kaggle.dao;

import com.google.gson.Gson;
import com.skuniv.cs.geonyeong.kaggle.service.EsClient;
import com.skuniv.cs.geonyeong.kaggle.vo.avro.AvroAccount;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

@Slf4j
@Repository
@RequiredArgsConstructor
public class AccountDao {

    private final static Gson gson = new Gson();
    private final String ACCOUNT_SCRIPT_SOURCE_PREFIX = "ctx._source.account.";
    private final String ACCOUNT_SCRIPT_PARAM_PREFIX = "params.";

    private final EsClient esClient;

    @Value("${com.skuniv.cs.geonyeong.kaggle.es.index.post}")
    private String esPostIndex;

    @Value("${com.skuniv.cs.geonyeong.kaggle.es.index.account}")
    private String esAccountIndex;

    @Value("${com.skuniv.cs.geonyeong.kaggle.es.type}")
    private String esType;

    public void deleteAccount(ConsumerRecords<String, AvroAccount> records) {
        BulkRequest bulkRequest = new BulkRequest();
        records.forEach(record -> {
            DeleteRequest deleteRequest = new DeleteRequest(esAccountIndex, esType,
                record.value().getId());
            bulkRequest.add(deleteRequest);
        });
        try {
            esClient.bulk(bulkRequest);
        } catch (RuntimeException e) {
            log.error("RuntimeException => {}", e);
        }
    }

    public void upsertAccount(ConsumerRecords<String, AvroAccount> records) {

        List<UpdateByQueryRequest> updateByQueryRequestList = new ArrayList<>();
        BulkRequest bulkRequest = new BulkRequest();

        records.forEach(record -> {
            AvroAccount avroAccount = record.value();

            // account index 수정
            IndexRequest indexRequest = new IndexRequest(esAccountIndex, esType,
                avroAccount.getId()).source(gson.toJson(avroAccount), XContentType.JSON);
            UpdateRequest updateRequest = new UpdateRequest(esAccountIndex, esType,
                avroAccount.getId())
                .doc(gson.toJson(avroAccount), XContentType.JSON)
                .upsert(indexRequest);
            bulkRequest.add(updateRequest);

            // post index 수정
            BoolQueryBuilder searchCommentLstQuery = QueryBuilders
                .boolQuery().must(QueryBuilders.matchQuery("account.id", avroAccount.getId()));
            Map<String, Object> parameters = gson.fromJson(gson.toJson(avroAccount), Map.class);

            Script script = new Script(ScriptType.INLINE, "painless",
                createScriptCodeByMap(parameters),
                parameters);
            UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest(esPostIndex);
            updateByQueryRequest.setDocTypes(esType);
            updateByQueryRequest.setScript(script);
            updateByQueryRequest.setQuery(searchCommentLstQuery);
            updateByQueryRequestList.add(updateByQueryRequest);
        });

        try {
            esClient.bulk(bulkRequest);
            updateByQueryRequestList.forEach(updateByQueryRequest -> {
                esClient.update(updateByQueryRequest);
            });
        } catch (RuntimeException e) {
            log.error("RuntimeException {}", e);
        }
    }

    private String createScriptCodeByMap(Map<String, Object> parameters) {
        StringBuilder sb = new StringBuilder();
        parameters.keySet().forEach(k -> {
            sb.append(
                ACCOUNT_SCRIPT_SOURCE_PREFIX + k + " = " + ACCOUNT_SCRIPT_PARAM_PREFIX + k + "; ");
        });
        log.info("script code => {}", sb.toString());
        return sb.toString();
    }
}

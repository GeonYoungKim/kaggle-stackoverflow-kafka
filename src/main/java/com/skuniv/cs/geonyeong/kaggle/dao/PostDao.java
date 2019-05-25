package com.skuniv.cs.geonyeong.kaggle.dao;

import com.google.gson.Gson;
import com.skuniv.cs.geonyeong.kaggle.service.EsClient;
import com.skuniv.cs.geonyeong.kaggle.vo.avro.AvroAnswer;
import com.skuniv.cs.geonyeong.kaggle.vo.avro.AvroQuestion;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.Script;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

@Slf4j
@Repository
@RequiredArgsConstructor
public class PostDao {

    private final static Gson gson = new Gson();

    @Value("${com.skuniv.cs.geonyeong.kaggle.es.index.post}")
    private String esIndex;

    @Value("${com.skuniv.cs.geonyeong.kaggle.es.type}")
    private String esType;

    private final EsClient esClient;

    private final String ANSWER_COUNT_PLUS_SCRIPT = "ctx._source.answerCount += 1";
    private final String ANSWER_COUNT_MINUST_SCRIPT = "ctx._source.answerCount -= 1";


    public void deleteAnswer(ConsumerRecords<String, AvroAnswer> records) {
        BulkRequest bulkRequest = new BulkRequest();
        records.forEach(item -> {
                DeleteRequest deleteRequest = new DeleteRequest(esIndex, esType, item.value().getId());
                UpdateRequest updateRequest = new UpdateRequest(esIndex, esType,
                    item.value().getParentId()).script(new Script(ANSWER_COUNT_MINUST_SCRIPT));
                bulkRequest.add(deleteRequest);
                bulkRequest.add(updateRequest);
            }
        );

        try {
            esClient.bulk(bulkRequest);
        } catch (RuntimeException e) {
            log.error("RuntimeException => {}", e);
        }
    }

    public void upsertAnswer(ConsumerRecords<String, AvroAnswer> records) {
        BulkRequest bulkRequest = new BulkRequest();
        records.forEach(item -> {
            AvroAnswer answer = item.value();
            IndexRequest indexRequest = new IndexRequest(esIndex, esType, answer.getId())
                .source(gson.toJson(answer), XContentType.JSON);
            UpdateRequest updateRequest = new UpdateRequest(esIndex, esType, answer.getParentId())
                .script(new Script(ANSWER_COUNT_PLUS_SCRIPT))
                .doc(gson.toJson(answer), XContentType.JSON)
                .upsert(indexRequest);
            bulkRequest.add(updateRequest);
        });

        try {
            esClient.bulk(bulkRequest);
        } catch (RuntimeException e) {
            log.error("RuntimeException {}", e);
        }
    }


    public void deleteQuestion(ConsumerRecords<String, AvroQuestion> records) {
        BulkRequest bulkRequest = new BulkRequest();
        records.forEach(item -> {
            DeleteRequest deleteRequest = new DeleteRequest(esIndex, esType, item.value().getId());
            bulkRequest.add(deleteRequest);
        });

        try {
            esClient.bulk(bulkRequest);
        } catch (RuntimeException e) {
            log.error("RuntimeException => {}", e);
        }
    }


    public void upsertQuestion(ConsumerRecords<String, AvroQuestion> records) {
        BulkRequest bulkRequest = new BulkRequest();
        records.forEach(item -> {
            AvroQuestion question = item.value();
            IndexRequest indexRequest = new IndexRequest(esIndex, esType, question.getId())
                .source(gson.toJson(question), XContentType.JSON).routing(question.getId());
            UpdateRequest updateRequest = new UpdateRequest(esIndex, esType, question.getId())
                .doc(gson.toJson(question), XContentType.JSON)
                .upsert(indexRequest);
            bulkRequest.add(updateRequest);
        });

        try {
            esClient.bulk(bulkRequest);
        } catch (RuntimeException e) {
            log.error("RuntimeException {}", e);
        }
    }
}

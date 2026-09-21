package com.jizuz.mcpserver.manager;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jizuz.mcpserver.models.mq.DocChunkVectorMsg;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.client.RestClientException;

import java.util.*;

@Slf4j
@Service
@RequiredArgsConstructor
public class QdrantRestDocManager {

    private final RestTemplate restTemplate;

    private final ObjectMapper objectMapper;

    @Value("${qdrant.rest-url}")
    private String qdrantRestUrl;

    @Value("${qdrant.collection-name}")
    private String collectionName;

    @Value("${qdrant.embedding-dimension}")
    private int embeddingDimension;

    /**
     * 初始化集合，不存在则创建
     */
    public void initCollection() {
        String url = qdrantRestUrl + "/collections/" + collectionName;
        try {
            // 判断集合是否存在
            restTemplate.getForObject(url, Map.class);
            log.info("Qdrant集合 {} 已存在", collectionName);
        } catch (RestClientException e) {
            log.info("集合不存在，开始创建：{}", collectionName);
            Map<String, Object> body = new HashMap<>();
            Map<String, Object> vectors = new HashMap<>();
            vectors.put("size", embeddingDimension);
            vectors.put("distance", "Cosine");
            body.put("vectors", vectors);
            restTemplate.put(url, body);
            log.info("集合 {} 创建成功", collectionName);
        }
    }

    /**
     * 批量写入向量（推荐，提升吞吐量）
     */
    public void batchUpsert(List<DocChunkVectorMsg> msgList) {
        if (msgList == null || msgList.isEmpty()) {
            return;
        }
        List<Map<String, Object>> points = new ArrayList<>();
        for (DocChunkVectorMsg msg : msgList) {
            Map<String, Object> payload = new HashMap<>();
            payload.put("docId", msg.getDocId());
            payload.put("chunkId", msg.getChunkId());
            payload.put("content", msg.getContent());
            payload.put("fileName", msg.getFileName());
            payload.put("pageNum", msg.getPageNum());
            payload.put("docType", msg.getDocType());

            Map<String, Object> point = new HashMap<>();
            point.put("id", UUID.randomUUID().toString());
            point.put("vector", msg.getVector());
            point.put("payload", payload);
            points.add(point);
        }

        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("points", points);
        String url = qdrantRestUrl + "/collections/" + collectionName + "/points";
        restTemplate.put(url, requestBody);
        log.info("批量写入Qdrant成功，数量:{}", points.size());
    }

    /**
     * 单条写入
     */
    public void upsertSingle(DocChunkVectorMsg msg) {
        batchUpsert(List.of(msg));
    }

    /**
     * 根据docId 删除该文档下所有向量（文档更新/删除场景）
     */
    public void deleteByDocId(String docId) {
        String url = qdrantRestUrl + "/collections/" + collectionName + "/points/delete";
        Map<String, Object> filter = Map.of(
                "must", List.of(Map.of(
                        "key", "docId",
                        "match", Map.of("value", docId)
                ))
        );
        Map<String, Object> body = Map.of("filter", filter);
        restTemplate.postForObject(url, body, String.class);
        log.info("删除docId={} 的所有向量成功", docId);
    }

    /**
     * 向量检索，RAG查询阶段使用
     * @param vector 查询向量
     * @param topN 返回条数
     * @return 匹配的chunk列表
     */
    public List<Map<String, Object>> search(List<Float> vector, int topN) {
        String url = qdrantRestUrl + "/collections/" + collectionName + "/points/search";
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("vector", vector);
        requestBody.put("limit", topN);
        requestBody.put("with_payload", true);
        Map<String,Object> resp = restTemplate.postForObject(url, requestBody, Map.class);
        return (List<Map<String, Object>>) resp.get("result");
    }
}
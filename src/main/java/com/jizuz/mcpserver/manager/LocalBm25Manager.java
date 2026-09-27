package com.jizuz.mcpserver.manager;

import com.jizuz.mcpserver.models.mq.DocChunkVectorMsg;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 本地BM25关键词索引（进程内实现，无外部依赖），用于RAG多路召回的关键词一路
 * 1. 启动时从Qdrant全量拉取chunk构建索引；MQ消费写入Qdrant成功后增量双写；
 * 2. 索引key与Qdrant point id对齐（写入端batchUpsert以chunkId作为point id），供RRF融合定位同一片段；
 * 3. 中文按二元组（bigram）、英文/数字按连续词元分词，统一小写归一化。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class LocalBm25Manager {

    /** BM25参数：词频饱和度 */
    private static final double K1 = 1.2;

    /** BM25参数：文档长度归一化强度 */
    private static final double B = 0.75;

    private final QdrantRestDocManager qdrantRestDocManager;

    /** pointId -> 索引文档 */
    private final Map<String, Bm25Doc> docMap = new HashMap<>();

    /** 倒排表：term -> pointId集合 */
    private final Map<String, Set<String>> postings = new HashMap<>();

    /** 全部文档的平均长度（token数） */
    private double avgDocLen = 1.0;

    /**
     * BM25检索单条结果
     */
    public record Bm25Hit(String pointId, double score, String content, String fileName, String docType) {}

    /**
     * 启动时全量构建索引（Qdrant不可用时仅告警，不阻断启动，可稍后通过 /rag/doc/rebuild-bm25 重建）
     */
    @EventListener(ApplicationReadyEvent.class)
    public void init() {
        try {
            log.info("BM25索引初始化完成，片段数:{}", rebuildFromQdrant());
        } catch (Exception e) {
            log.warn("BM25索引初始化失败（Qdrant不可用），可通过 POST /rag/doc/rebuild-bm25 手动重建：{}", e.getMessage());
        }
    }

    /**
     * 从Qdrant全量拉取并重建索引
     * @return 索引片段数
     */
    public synchronized int rebuildFromQdrant() {
        List<Map<String, Object>> points = qdrantRestDocManager.scrollAllPoints();
        docMap.clear();
        postings.clear();
        avgDocLen = 1.0;
        for (Map<String, Object> point : points) {
            Map<String, Object> payload = payloadOf(point);
            index(String.valueOf(point.get("id")),
                    str(payload.get("docId")),
                    str(payload.get("content")),
                    str(payload.get("fileName")),
                    str(payload.get("docType")));
        }
        log.info("BM25索引重建完成，片段数:{}", docMap.size());
        return docMap.size();
    }

    /**
     * MQ消费写入Qdrant成功后，增量写入本地BM25索引（chunkId即Qdrant point id）
     */
    public synchronized void addChunks(List<DocChunkVectorMsg> msgList) {
        if (msgList == null || msgList.isEmpty()) {
            return;
        }
        for (DocChunkVectorMsg msg : msgList) {
            index(msg.getChunkId(), msg.getDocId(), msg.getContent(), msg.getFileName(), msg.getDocType());
        }
    }

    /**
     * 按docId移除该文档全部片段（与Qdrant deleteByDocId对应）
     */
    public synchronized void removeByDocId(String docId) {
        List<String> removed = docMap.values().stream()
                .filter(d -> docId.equals(d.docId()))
                .map(Bm25Doc::pointId)
                .toList();
        for (String pointId : removed) {
            removeInternal(pointId);
        }
    }

    /**
     * BM25关键词检索，返回按得分降序的TopN
     */
    public synchronized List<Bm25Hit> search(String query, int topN) {
        List<String> qTerms = tokenize(query);
        if (qTerms.isEmpty() || docMap.isEmpty()) {
            return List.of();
        }
        int n = docMap.size();
        Map<String, Double> scores = new HashMap<>();
        for (String term : qTerms) {
            Set<String> hitIds = postings.get(term);
            if (hitIds == null || hitIds.isEmpty()) {
                continue;
            }
            double df = hitIds.size();
            double idf = Math.log(1.0 + (n - df + 0.5) / (df + 0.5));
            for (String pointId : hitIds) {
                Bm25Doc doc = docMap.get(pointId);
                Integer freq = doc.tf().get(term);
                if (freq == null) {
                    continue;
                }
                double tf = freq;
                double denom = tf + K1 * (1.0 - B + B * doc.len() / avgDocLen);
                scores.merge(pointId, idf * tf * (K1 + 1.0) / denom, Double::sum);
            }
        }
        return scores.entrySet().stream()
                .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
                .limit(topN)
                .map(e -> {
                    Bm25Doc doc = docMap.get(e.getKey());
                    return new Bm25Hit(doc.pointId(), e.getValue(), doc.content(), doc.fileName(), doc.docType());
                })
                .toList();
    }

    /**
     * 当前索引片段数
     */
    public synchronized int size() {
        return docMap.size();
    }

    // ========== 内部：索引维护 ==========

    private void index(String pointId, String docId, String content, String fileName, String docType) {
        if (pointId == null || pointId.isBlank() || content == null || content.isBlank()) {
            return;
        }
        removeInternal(pointId);
        List<String> terms = tokenize(content);
        Map<String, Integer> tf = new HashMap<>();
        for (String term : terms) {
            tf.merge(term, 1, Integer::sum);
        }
        docMap.put(pointId, new Bm25Doc(pointId, docId, content, fileName, docType, tf, terms.size()));
        for (String term : tf.keySet()) {
            postings.computeIfAbsent(term, k -> new HashSet<>()).add(pointId);
        }
        avgDocLen = docMap.values().stream().mapToInt(Bm25Doc::len).average().orElse(1.0);
    }

    private void removeInternal(String pointId) {
        Bm25Doc old = docMap.remove(pointId);
        if (old == null) {
            return;
        }
        for (String term : old.tf().keySet()) {
            Set<String> ids = postings.get(term);
            if (ids != null) {
                ids.remove(pointId);
                if (ids.isEmpty()) {
                    postings.remove(term);
                }
            }
        }
        if (!docMap.isEmpty()) {
            avgDocLen = docMap.values().stream().mapToInt(Bm25Doc::len).average().orElse(1.0);
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> payloadOf(Map<String, Object> point) {
        Object payload = point.get("payload");
        return payload instanceof Map ? (Map<String, Object>) payload : Map.of();
    }

    private String str(Object value) {
        return value == null ? "" : String.valueOf(value);
    }

    // ========== 分词 ==========

    /**
     * 中英文混合分词：中文滑动二元组（bigram），英文/数字连续段作为词元，统一小写
     */
    static List<String> tokenize(String text) {
        List<String> tokens = new ArrayList<>();
        if (text == null || text.isEmpty()) {
            return tokens;
        }
        String s = text.toLowerCase();
        int n = s.length();
        StringBuilder latin = new StringBuilder();
        for (int i = 0; i < n; i++) {
            char c = s.charAt(i);
            if (isCjk(c)) {
                flushLatin(tokens, latin);
                if (i + 1 < n && isCjk(s.charAt(i + 1))) {
                    tokens.add(s.substring(i, i + 2));
                } else {
                    tokens.add(String.valueOf(c));
                }
            } else if (Character.isLetterOrDigit(c)) {
                latin.append(c);
            } else {
                flushLatin(tokens, latin);
            }
        }
        flushLatin(tokens, latin);
        return tokens;
    }

    private static boolean isCjk(char c) {
        return Character.UnicodeScript.of(c) == Character.UnicodeScript.HAN;
    }

    private static void flushLatin(List<String> tokens, StringBuilder latin) {
        if (latin.length() > 0) {
            tokens.add(latin.toString());
            latin.setLength(0);
        }
    }

    /**
     * 索引文档（不可变）
     */
    private record Bm25Doc(String pointId, String docId, String content, String fileName, String docType,
                           Map<String, Integer> tf, int len) {}

}

package com.jizuz.mcpserver.manager;

import dev.langchain4j.model.embedding.EmbeddingModel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 消费端双索引同步（本地BM25 + Qdrant），处理ADD/UPDATE/DELETE三种事件：
 * 1. ADD：分块 → BM25增量追加 → embedding → Qdrant批量upsert（BM25不依赖embedding先行写入）；
 * 2. UPDATE：按docId删双索引旧数据后按ADD重写；
 * 3. DELETE：按docId删双索引（进程内BM25即时删除，秒级不可检索）。
 * point id = docId_分块序号，与权威源kb_doc行chunk_id、BM25索引key三方对齐。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class KbDocIndexManager {

    private final EmbeddingModel embeddingModel;
    private final QdrantRestDocManager qdrantRestDocManager;
    private final LocalBm25Manager localBm25Manager;

    @Value("${embedding.chunk.size}")
    private int chunkSize;

    @Value("${embedding.chunk.overlap}")
    private int chunkOverlap;

    /**
     * ADD：新文档同步双索引（BM25先写保证关键词路尽快可检索，再embedding写Qdrant）
     */
    public void addDoc(String docId, String docGroup, String title, String content) {
        List<String> chunks = ChunkSplitter.split(content, chunkSize, chunkOverlap);
        // 1. BM25增量追加
        localBm25Manager.addDocChunks(docId, docGroup, title, chunks);
        // 2. embedding + Qdrant批量upsert
        upsertDocChunks(docId, docGroup, title, chunks);
        log.info("ADD双索引同步完成 docId={}, group={}, chunks={}", docId, docGroup, chunks.size());
    }

    /**
     * UPDATE：删旧写新（Qdrant按docId删旧向量、写新向量；BM25移除旧片段后重写）
     */
    public void updateDoc(String docId, String docGroup, String title, String content) {
        removeDoc(docId);
        addDoc(docId, docGroup, title, content);
        log.info("UPDATE双索引同步完成 docId={}", docId);
    }

    /**
     * DELETE：双索引按docId移除
     */
    public void removeDoc(String docId) {
        qdrantRestDocManager.deleteByDocId(docId);
        localBm25Manager.removeByDocId(docId);
        log.info("DELETE双索引移除完成 docId={}", docId);
    }

    /**
     * Qdrant重灌专用：按权威源已分块内容重算向量并upsert（不动BM25，避免重复写）
     */
    public void reembedDoc(String docId, String docGroup, String title, List<String> chunks) {
        upsertDocChunks(docId, docGroup, title, chunks);
    }

    /**
     * 逐块embedding后批量upsert到Qdrant（payload带docId/docGroup/title，point id=docId_序号）
     */
    private void upsertDocChunks(String docId, String docGroup, String title, List<String> chunks) {
        List<Map<String, Object>> points = new ArrayList<>(chunks.size());
        for (int i = 0; i < chunks.size(); i++) {
            points.add(buildPoint(docId, docGroup, title, i, chunks.get(i)));
        }
        qdrantRestDocManager.upsertRawPoints(points);
    }

    private Map<String, Object> buildPoint(String docId, String docGroup, String title, int chunkIdx, String chunk) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("docId", docId);
        payload.put("docGroup", docGroup);
        payload.put("title", title);
        payload.put("chunkIdx", chunkIdx);
        payload.put("content", chunk);

        Map<String, Object> point = new HashMap<>();
        point.put("id", docId + "_" + chunkIdx);
        point.put("vector", embed(chunk));
        point.put("payload", payload);
        return point;
    }

    /**
     * 文本向量化（float[]转List<Float>）
     */
    private List<Float> embed(String text) {
        float[] array = embeddingModel.embed(text).content().vector();
        List<Float> vector = new ArrayList<>(array.length);
        for (float f : array) {
            vector.add(f);
        }
        return vector;
    }
}

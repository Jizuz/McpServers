package com.jizuz.mcpserver.tools;

import com.jizuz.mcpserver.manager.LocalBm25Manager;
import com.jizuz.mcpserver.manager.QdrantRestDocManager;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.output.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.when;

/**
 * RagTool 单元测试：验证向量+BM25多路召回、RRF融合排序、降级兜底、TopK边界逻辑
 */
@ExtendWith(MockitoExtension.class)
class RagToolTest {

    private static final int VECTOR_TOP_N = 10;
    private static final int BM25_TOP_N = 10;
    private static final int RRF_K = 60;

    @Mock
    private EmbeddingModel embeddingModel;

    @Mock
    private QdrantRestDocManager qdrantRestDocManager;

    @Mock
    private LocalBm25Manager localBm25Manager;

    private RagTool ragTool;

    @BeforeEach
    void setUp() {
        ragTool = new RagTool(embeddingModel, qdrantRestDocManager, localBm25Manager,
                VECTOR_TOP_N, BM25_TOP_N, RRF_K);
    }

    private void mockEmbed(float... vector) {
        when(embeddingModel.embed(anyString())).thenReturn(Response.from(new Embedding(vector)));
    }

    private Map<String, Object> vectorPoint(String id, double score, String content, String fileName) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("content", content);
        payload.put("fileName", fileName);
        Map<String, Object> point = new HashMap<>();
        point.put("id", id);
        point.put("score", score);
        point.put("payload", payload);
        return point;
    }

    private LocalBm25Manager.Bm25Hit bm25Hit(String id, double score, String content) {
        return new LocalBm25Manager.Bm25Hit(id, id, null, score, content, "bm25-source.md", "md");
    }

    @Test
    void searchKnowledgeBase_rrfFusionPreferDualHit() {
        mockEmbed(0.1f, 0.2f);
        // 向量路排名：B第一、A第二；BM25路排名：A第一、C第二 → A双路命中，RRF应排第一
        when(qdrantRestDocManager.search(anyList(), eq(VECTOR_TOP_N), isNull())).thenReturn(List.of(
                vectorPoint("idB", 0.90, "内容B", "b.md"),
                vectorPoint("idA", 0.80, "内容A", "a.md")));
        when(localBm25Manager.search(anyString(), eq(BM25_TOP_N), isNull())).thenReturn(List.of(
                bm25Hit("idA", 9.5, "内容A"),
                bm25Hit("idC", 5.0, "内容C")));

        String result = ragTool.searchKnowledgeBase("胃炎症状", 3, null);

        assertTrue(result.contains("RRF融合"));
        assertTrue(result.contains("向量+关键词"));
        // A双路命中RRF最高，应排在单路命中的B之前
        assertTrue(result.indexOf("内容A") < result.indexOf("内容B"), "双路命中应排前");
        assertTrue(result.contains("内容C"), "BM25独有结果也应保留");
    }

    @Test
    void searchKnowledgeBase_degradeToBm25WhenEmbeddingFail() {
        when(embeddingModel.embed(anyString())).thenThrow(new RuntimeException("DashScope欠费"));
        when(localBm25Manager.search(anyString(), eq(BM25_TOP_N), isNull())).thenReturn(List.of(
                bm25Hit("idA", 9.5, "BM25命中的内容")));

        String result = ragTool.searchKnowledgeBase("胃炎症状", 3, null);

        assertTrue(result.contains("仅BM25关键词召回"), "应提示降级");
        assertTrue(result.contains("BM25命中的内容"), "应返回BM25结果");
        assertTrue(result.contains("命中:关键词"));
    }

    @Test
    void searchKnowledgeBase_bothRecallFail() {
        when(embeddingModel.embed(anyString())).thenThrow(new RuntimeException("向量服务不可用"));
        when(localBm25Manager.search(anyString(), eq(BM25_TOP_N), isNull())).thenThrow(new RuntimeException("BM25不可用"));

        String result = ragTool.searchKnowledgeBase("任意问题", 3, null);

        assertTrue(result.contains("均不可用"));
    }

    @Test
    void searchKnowledgeBase_emptyResult() {
        mockEmbed(0.1f);
        when(qdrantRestDocManager.search(anyList(), eq(VECTOR_TOP_N), isNull())).thenReturn(List.of());
        when(localBm25Manager.search(anyString(), eq(BM25_TOP_N), isNull())).thenReturn(List.of());

        String result = ragTool.searchKnowledgeBase("不存在的问题", 3, null);

        assertTrue(result.contains("未检索到相关内容"));
    }

    @Test
    void searchKnowledgeBase_topKBoundary() {
        mockEmbed(0.1f);
        when(qdrantRestDocManager.search(anyList(), eq(VECTOR_TOP_N), isNull())).thenReturn(List.of(
                vectorPoint("id1", 0.9, "内容一", "1.md"),
                vectorPoint("id2", 0.8, "内容二", "2.md"),
                vectorPoint("id3", 0.7, "内容三", "3.md")));
        when(localBm25Manager.search(anyString(), eq(BM25_TOP_N), isNull())).thenReturn(List.of());

        // topK>5截断为5：3条召回结果全部保留
        String r1 = ragTool.searchKnowledgeBase("问题", 10, null);
        assertTrue(r1.contains("内容一"));
        assertTrue(r1.contains("内容三"));

        // topK非法重置为默认3
        String r2 = ragTool.searchKnowledgeBase("问题", 0, null);
        assertTrue(r2.contains("内容三"));

        // topK=2：仅保留RRF最高的前2条
        String r3 = ragTool.searchKnowledgeBase("问题", 2, null);
        assertTrue(r3.contains("内容一"));
        assertTrue(r3.contains("内容二"));
        assertFalse(r3.contains("内容三"));
    }

    @Test
    void searchKnowledgeBase_blankQuery() {
        String result = ragTool.searchKnowledgeBase("  ", 3, null);
        assertEquals("检索失败：查询内容不能为空", result);
    }

    @Test
    void searchKnowledgeBase_illegalDocGroup() {
        // 非法分组直接返回提示，不发起任何召回
        String result = ragTool.searchKnowledgeBase("胃炎症状", 3, "illegal_group");
        assertTrue(result.contains("非法文档分组"));
        assertTrue(result.contains("illegal_group"));
    }

}

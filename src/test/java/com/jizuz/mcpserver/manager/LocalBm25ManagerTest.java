package com.jizuz.mcpserver.manager;

import com.jizuz.mcpserver.dao.KbDocDao;
import com.jizuz.mcpserver.dao.Entity.KbDoc;
import com.jizuz.mcpserver.models.mq.DocChunkVectorMsg;
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
import static org.mockito.Mockito.when;

/**
 * LocalBm25Manager 单元测试：分词、相关性排序、删除、重建逻辑
 */
@ExtendWith(MockitoExtension.class)
class LocalBm25ManagerTest {

    @Mock
    private QdrantRestDocManager qdrantRestDocManager;

    @Mock
    private KbDocDao kbDocDao;

    private LocalBm25Manager manager;

    @BeforeEach
    void setUp() {
        manager = new LocalBm25Manager(qdrantRestDocManager, kbDocDao);
    }

    private DocChunkVectorMsg msg(String chunkId, String docId, String content, String fileName) {
        return DocChunkVectorMsg.builder()
                .docId(docId).chunkId(chunkId).content(content)
                .fileName(fileName).docType("md").build();
    }

    @Test
    void tokenize_cnBigramAndEnWord() {
        List<String> tokens = LocalBm25Manager.tokenize("胃炎Gastritis症状2024");
        assertTrue(tokens.contains("胃炎"), "中文bigram分词");
        assertTrue(tokens.contains("gastritis"), "英文词元小写化");
        assertTrue(tokens.contains("2024"), "数字词元");
    }

    @Test
    void search_rankMostRelevantFirst() {
        manager.addChunks(List.of(
                msg("c1", "d1", "胃炎是胃黏膜的炎症反应，典型症状包括上腹痛、腹胀", "health.md"),
                msg("c2", "d2", "感冒通常由病毒引起，症状有鼻塞、流涕", "cold.md"),
                msg("c3", "d3", "膝关节疼痛常见于运动损伤", "sport.md")));

        List<LocalBm25Manager.Bm25Hit> hits = manager.search("胃炎症状", 3);

        assertFalse(hits.isEmpty());
        // c1同时命中胃炎/炎症/症状多个词元，应排第一
        assertEquals("c1", hits.get(0).pointId());
        assertTrue(hits.get(0).content().contains("胃炎"));
    }

    @Test
    void search_emptyQueryOrNoHit() {
        manager.addChunks(List.of(msg("c1", "d1", "任意内容", "a.md")));
        assertTrue(manager.search("", 3).isEmpty());
        assertTrue(manager.search("不相关的查询词组xyzq", 3).isEmpty());
    }

    @Test
    void removeByDocId() {
        manager.addChunks(List.of(
                msg("c1", "d1", "胃炎症状内容", "a.md"),
                msg("c2", "d1", "胃炎护理内容", "a.md"),
                msg("c3", "d2", "感冒症状内容", "b.md")));
        manager.removeByDocId("d1");

        assertTrue(manager.search("胃炎", 3).isEmpty());
        assertEquals(1, manager.size());
    }

    @Test
    void rebuildFromQdrant() {
        Map<String, Object> payload = new HashMap<>();
        payload.put("docId", "d9");
        payload.put("content", "胃炎是胃黏膜的炎症反应");
        payload.put("fileName", "health.md");
        payload.put("docType", "webpage");
        Map<String, Object> point = new HashMap<>();
        point.put("id", "p9");
        point.put("payload", payload);
        when(qdrantRestDocManager.scrollAllPoints()).thenReturn(List.of(point));

        int size = manager.rebuildFromQdrant();

        assertEquals(1, size);
        assertEquals("p9", manager.search("胃炎炎症", 1).get(0).pointId());
    }

    @Test
    void addDocChunks_pointIdAlignedWithChunkIdx() {
        // 权威源链路：pointId = docId_分块序号，title作为来源名
        manager.addDocChunks("d1", "cardio_health", "高血压科普", List.of("高血压的日常管理", "高血压的饮食原则"));

        List<LocalBm25Manager.Bm25Hit> hits = manager.search("高血压饮食", 3);
        assertFalse(hits.isEmpty());
        assertEquals("d1_1", hits.get(0).pointId(), "第二块pointId应为docId_1");
        assertEquals("d1", hits.get(0).docId());
        assertEquals("cardio_health", hits.get(0).docGroup());
        assertEquals("高血压科普", hits.get(0).fileName());
    }

    @Test
    void search_filterByDocGroup() {
        manager.addDocChunks("d1", "cardio_health", "心血管文档", List.of("胸痛胸闷可能是心绞痛的典型表现"));
        manager.addDocChunks("d2", "resp_health", "呼吸文档", List.of("胸痛胸闷伴咳嗽常见于呼吸系统疾病"));

        // 全库检索：两分组均可命中
        assertEquals(2, manager.search("胸痛胸闷", 5).size());
        // 分组过滤：仅命中心血管分组
        List<LocalBm25Manager.Bm25Hit> hits = manager.search("胸痛胸闷", 5, "cardio_health");
        assertEquals(1, hits.size());
        assertEquals("d1", hits.get(0).docId());
        // 过滤分组无匹配文档时返回空
        assertTrue(manager.search("胸痛胸闷", 5, "ped_health").isEmpty());
    }

    @Test
    void rebuildFromDb() {
        when(kbDocDao.listValidChunks()).thenReturn(List.of(
                KbDoc.builder().docId("d1").docGroup("endo_health").title("糖尿病科普")
                        .content("糖尿病患者的血糖管理").chunkId(0).contentMd5("m1").status(1).build(),
                KbDoc.builder().docId("d1").docGroup("endo_health").title("糖尿病科普")
                        .content("糖尿病的饮食控制").chunkId(1).contentMd5("m1").status(1).build(),
                KbDoc.builder().docId("d2").docGroup("women_health").title("孕期科普")
                        .content("孕期营养补充建议").chunkId(0).contentMd5("m2").status(1).build()));

        int size = manager.rebuildFromDb();

        assertEquals(3, size);
        // 分块pointId与kb_doc行chunk_id对齐：d1_0 / d1_1
        assertEquals("d1_1", manager.search("饮食控制", 1).get(0).pointId());
        // 软删行不在重建范围外（listValidChunks只返回status=1），重建后仅有效文档
        assertEquals("d2", manager.search("孕期营养", 1, "women_health").get(0).docId());
    }

}

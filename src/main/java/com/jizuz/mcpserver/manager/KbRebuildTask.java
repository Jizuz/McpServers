package com.jizuz.mcpserver.manager;

import com.jizuz.mcpserver.dao.KbDocDao;
import com.jizuz.mcpserver.dao.Entity.KbDoc;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 兜底重建定时任务（最终一致性的最后一层）：
 * 1. 每日凌晨：BM25从权威源全量重建——修复漏消费/乱序/死信导致的BM25偏差，并重算全局IDF；
 * 2. 每周低峰：Qdrant全量重灌——清空后从权威源重写，消除HNSW碎片与脏数据（依赖embedding可用，配置开关控制，默认关闭）。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class KbRebuildTask {

    private final LocalBm25Manager localBm25Manager;
    private final QdrantRestDocManager qdrantRestDocManager;
    private final KbDocIndexManager kbDocIndexManager;
    private final KbDocDao kbDocDao;

    @Value("${rag.rebuild.qdrant-enabled:false}")
    private boolean qdrantRebuildEnabled;

    /**
     * 每日凌晨3点：BM25从权威源全量重建（锁内原子替换，重建期间可正常检索旧索引）
     */
    @Scheduled(cron = "${rag.rebuild.bm25-cron:0 0 3 * * ?}")
    public void rebuildBm25Daily() {
        try {
            localBm25Manager.rebuildFromDb();
            log.info("每日BM25全量重建完成");
        } catch (Exception e) {
            log.error("每日BM25全量重建失败", e);
        }
    }

    /**
     * 每周日凌晨4:30：Qdrant全量重灌（清空→权威源逐块重embedding写回；不动BM25）
     */
    @Scheduled(cron = "${rag.rebuild.qdrant-cron:0 30 4 * * SUN}")
    public void rebuildQdrantWeekly() {
        if (!qdrantRebuildEnabled) {
            log.info("Qdrant周重建开关关闭，跳过");
            return;
        }
        try {
            long start = System.currentTimeMillis();
            qdrantRestDocManager.deleteAllPoints();
            Map<String, List<KbDoc>> byDoc = kbDocDao.listValidChunks().stream()
                    .collect(Collectors.groupingBy(KbDoc::getDocId, LinkedHashMap::new, Collectors.toList()));
            for (List<KbDoc> chunks : byDoc.values()) {
                KbDoc head = chunks.get(0);
                kbDocIndexManager.reembedDoc(head.getDocId(), head.getDocGroup(), head.getTitle(),
                        chunks.stream().map(KbDoc::getContent).toList());
            }
            log.info("每周Qdrant全量重灌完成，文档数:{}，耗时:{}ms", byDoc.size(), System.currentTimeMillis() - start);
        } catch (Exception e) {
            log.error("每周Qdrant全量重灌失败", e);
        }
    }
}

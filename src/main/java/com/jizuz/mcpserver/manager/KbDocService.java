package com.jizuz.mcpserver.manager;

import com.jizuz.mcpserver.dao.KbDocDao;
import com.jizuz.mcpserver.models.enums.DocGroupEnum;
import com.jizuz.mcpserver.dao.Entity.KbDoc;
import com.jizuz.mcpserver.models.mq.RagDocChangeEvent;
import com.jizuz.mcpserver.mq.producer.DocVectorProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.DigestUtils;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * 权威源唯一写入口（所有文档变更必须经过本服务）：
 * 先落库 → 再判定事件类型（md5/分组/标题任一变化才发UPDATE）→ 发MQ事件。
 * 业务层无权指定事件类型，杜绝"内容没变也广播重建"。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class KbDocService {

    private final KbDocDao kbDocDao;
    private final DocVectorProducer docVectorProducer;

    @Value("${embedding.chunk.size}")
    private int chunkSize;

    @Value("${embedding.chunk.overlap}")
    private int chunkOverlap;

    /**
     * 上传新文档：生成UUID docId → 分块落库 → 发ADD事件（先落库后发消息）
     * @return 文档ID（全链路唯一不变）
     */
    public String uploadDoc(String title, String docGroup, String content) {
        validate(docGroup, title, content);
        String docId = UUID.randomUUID().toString();
        String md5 = md5(content);
        insertDocRows(docId, docGroup, title, content, md5);
        try {
            sendEvent(docId, docGroup, title, content, md5, RagDocChangeEvent.EVENT_ADD);
        } catch (Exception e) {
            // 上传场景严格一致：MQ发送失败回滚刚插入的行，调用方报错后可安全重传
            kbDocDao.deleteRowsByDocId(docId);
            throw new IllegalStateException("文档事件发送失败，已回滚落库，请稍后重试", e);
        }
        return docId;
    }

    /**
     * 编辑文档：md5/标题/分组任一变化才更新落库并发UPDATE（内容与元数据均无变化则不发消息）
     * @return true=已触发UPDATE事件；false=无变化未发消息
     */
    public boolean updateDoc(String docId, String title, String docGroup, String content) {
        validate(docGroup, title, content);
        List<KbDoc> oldRows = kbDocDao.findValidChunks(docId);
        if (oldRows.isEmpty()) {
            throw new IllegalArgumentException("文档不存在或已删除：" + docId);
        }
        KbDoc head = oldRows.get(0);
        String newMd5 = md5(content);
        boolean contentChanged = !newMd5.equals(head.getContentMd5());
        boolean metaChanged = !Objects.equals(head.getTitle(), title) || !Objects.equals(head.getDocGroup(), docGroup);
        if (!contentChanged && !metaChanged) {
            log.info("文档内容与元数据均无变化，不发送UPDATE事件 docId={}", docId);
            return false;
        }
        // 替换式更新：物理删旧行（新分块数可能与旧不同）后插入新行
        kbDocDao.deleteRowsByDocId(docId);
        insertDocRows(docId, docGroup, title, content, newMd5);
        try {
            sendEvent(docId, docGroup, title, content, newMd5, RagDocChangeEvent.EVENT_UPDATE);
        } catch (Exception e) {
            // 落库已成功，索引差异由每日BM25全量重建兜底收敛（权威源仍是最新的）
            log.error("UPDATE事件发送失败（权威源已更新，等待兜底重建收敛） docId={}", docId, e);
        }
        return true;
    }

    /**
     * 删除文档：软删除（status=0，可恢复）→ 发DELETE事件（双索引即时移除）
     */
    public void deleteDoc(String docId) {
        if (kbDocDao.findAnyChunks(docId).isEmpty()) {
            throw new IllegalArgumentException("文档不存在：" + docId);
        }
        kbDocDao.softDeleteByDocId(docId);
        try {
            sendEvent(docId, null, null, null, null, RagDocChangeEvent.EVENT_DELETE);
        } catch (Exception e) {
            log.error("DELETE事件发送失败（权威源已软删，等待兜底重建收敛） docId={}", docId, e);
        }
    }

    /**
     * 恢复软删文档：status重置1 → 重发ADD（消费端按权威源当前内容重建双索引）
     */
    public void restoreDoc(String docId) {
        List<KbDoc> rows = kbDocDao.findAnyChunks(docId);
        if (rows.isEmpty()) {
            throw new IllegalArgumentException("文档不存在：" + docId);
        }
        if (rows.get(0).getStatus() != null && rows.get(0).getStatus() == 1) {
            throw new IllegalArgumentException("文档未被删除，无需恢复：" + docId);
        }
        kbDocDao.restoreByDocId(docId);
        KbDoc head = rows.get(0);
        try {
            sendEvent(head.getDocId(), head.getDocGroup(), head.getTitle(), null, head.getContentMd5(),
                    RagDocChangeEvent.EVENT_ADD);
        } catch (Exception e) {
            log.error("恢复ADD事件发送失败（权威源已恢复，等待兜底重建收敛） docId={}", docId, e);
        }
    }

    // ========== 内部：落库与事件发送 ==========

    private void validate(String docGroup, String title, String content) {
        if (!DocGroupEnum.isValid(docGroup)) {
            throw new IllegalArgumentException("非法文档分组：" + docGroup
                    + "（可选：cardio_health/resp_health/ped_health/endo_health/women_health/common_living）");
        }
        if (title == null || title.isBlank()) {
            throw new IllegalArgumentException("文档标题不能为空");
        }
        if (content == null || content.isBlank()) {
            throw new IllegalArgumentException("文档正文不能为空");
        }
    }

    /**
     * 分块落库（与消费端索引写入共用ChunkSplitter，保证kb_doc行chunk_id与索引point id对齐）
     */
    private void insertDocRows(String docId, String docGroup, String title, String content, String md5) {
        List<String> chunks = ChunkSplitter.split(content, chunkSize, chunkOverlap);
        List<KbDoc> rows = new ArrayList<>(chunks.size());
        for (int i = 0; i < chunks.size(); i++) {
            rows.add(KbDoc.builder()
                    .docId(docId).docGroup(docGroup).title(title)
                    .content(chunks.get(i)).chunkId(i).contentMd5(md5).status(1)
                    .build());
        }
        kbDocDao.insertChunks(rows);
        log.info("权威源落库完成 docId={}, group={}, chunks={}", docId, docGroup, rows.size());
    }

    private void sendEvent(String docId, String docGroup, String title, String content, String md5, String eventType) throws Exception {
        RagDocChangeEvent event = RagDocChangeEvent.builder()
                .docId(docId).docGroup(docGroup).title(title).content(content)
                .chunkId(0).contentMd5(md5)
                .updateTs(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))
                .eventType(eventType)
                .msgId(UUID.randomUUID().toString())
                .build();
        docVectorProducer.sendDocChangeEvent(event);
    }

    private String md5(String content) {
        return DigestUtils.md5DigestAsHex(content.getBytes(StandardCharsets.UTF_8));
    }
}
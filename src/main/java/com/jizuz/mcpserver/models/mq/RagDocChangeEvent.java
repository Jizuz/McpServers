package com.jizuz.mcpserver.models.mq;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 文档变更事件（doc级消息）
 * topic沿用 doc_chunk_vector_topic，tag = TAG_ADD / TAG_UPDATE / TAG_DELETE 与旧链路vector_write隔离；
 * content为整篇正文，消费端统一分块后同步双索引（chunkId恒为0）
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RagDocChangeEvent {

    public static final String EVENT_ADD = "ADD";
    public static final String EVENT_UPDATE = "UPDATE";
    public static final String EVENT_DELETE = "DELETE";

    /** 文档唯一ID（UUID，分区键保证同一文档事件顺序消费） */
    private String docId;

    /** 文档分组（DocGroupEnum.code） */
    private String docGroup;

    /** 文档标题 */
    private String title;

    /** 整篇正文（消费端统一分块） */
    private String content;

    /** 分块序号（doc级消息恒为0） */
    private Integer chunkId;

    /** 整篇正文MD5（消费端与权威源二次比对，防乱序旧消息覆盖） */
    private String contentMd5;

    /** 变更时间 yyyy-MM-dd HH:mm:ss */
    private String updateTs;

    /** 事件类型：ADD / UPDATE / DELETE */
    private String eventType;

    /** 全局唯一消息ID（幂等去重，非RocketMQ消息id） */
    private String msgId;
}

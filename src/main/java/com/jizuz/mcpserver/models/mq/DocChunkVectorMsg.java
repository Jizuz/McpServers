package com.jizuz.mcpserver.models.mq;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DocChunkVectorMsg {

    /** 文档唯一ID */
    private String docId;

    /** 分片ID */
    private String chunkId;

    /** 原文片段 */
    private String content;

    /** 向量数组 */
    private List<Float> vector;

    /** 文档名称 */
    private String fileName;

    /** 页码/段落号 */
    private Integer pageNum;

    /** 文档类型 pdf/md */
    private String docType;

}

package com.jizuz.mcpserver.dao.Entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * 权威源kb_doc表实体（每行=一个文档分块）
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KbDoc {

    /** 主键ID */
    private Long id;

    /** 文档唯一ID（UUID，上传生成后全链路不变） */
    private String docId;

    /** 文档分组（DocGroupEnum.code） */
    private String docGroup;

    /** 文档标题 */
    private String title;

    /** 分块正文 */
    private String content;

    /** 分块序号（分块唯一ID = docId + "_" + chunkId） */
    private Integer chunkId;

    /** 整篇正文MD5（变更判定依据，同文档所有块相同） */
    private String contentMd5;

    /** 1有效 / 0已删除（软删除） */
    private Integer status;

    private LocalDateTime gmtCreate;

    private LocalDateTime gmtUpdate;
}

-- 知识库权威源表：所有文档变更先落库（先写源、后发MQ），每行=一个文档分块
CREATE TABLE IF NOT EXISTS kb_doc (
  id           BIGINT        NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  doc_id       VARCHAR(64)   NOT NULL                COMMENT '文档ID（UUID，全链路唯一不变）',
  doc_group    VARCHAR(32)   NOT NULL                COMMENT '文档分组：cardio_health/resp_health/ped_health/endo_health/women_health/common_living',
  title        VARCHAR(512)  NOT NULL                COMMENT '文档标题',
  content      LONGTEXT      NOT NULL                COMMENT '文档正文（或正文分块）',
  chunk_id     INT           NOT NULL DEFAULT 0      COMMENT '分块序号，不分块恒为 0',
  content_md5  CHAR(32)      NOT NULL                COMMENT '整篇正文MD5，变更判定依据（同文档所有块相同）',
  status       TINYINT       NOT NULL DEFAULT 1      COMMENT '1 有效 / 0 已删除（软删除）',
  gmt_create   DATETIME      DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  gmt_update   DATETIME      DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (id),
  KEY idx_doc_id (doc_id),
  KEY idx_group_status (doc_group, status),
  KEY idx_status_ts (status, gmt_update)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='知识库文档';
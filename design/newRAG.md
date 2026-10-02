# 本地BM25 \+ 向量库知识库实时更新系统设计方案（RocketMQ事件驱动）

## 一、方案概述

本方案面向 RAG 知识库的**文档生命周期管理**：覆盖文档上传、编辑、删除、检索全流程，实现文档变更**秒级同步**到本地BM25稀疏索引与Qdrant稠密向量库，并通过定时全量重建兜底保证长期数据一致。

- **检索组件**：本地BM25内存索引（稀疏召回）\+ Qdrant向量库（稠密召回），两路按 doc\_id 对齐后 RRF 融合

- **变更驱动**：RocketMQ 事件驱动，文档变更即刻触发索引更新，废弃定时轮询

- **主键规范**：上传时生成全局 UUID 作为 doc\_id，全链路唯一、永久不变

- **兜底机制**：每日 BM25 全量重建 \+ 每周 Qdrant 双集合切换，修复漏消息、乱序、索引碎片

## 二、系统总体架构

系统共分四层：操作入口层、权威源层、事件分发层、双索引同步层。

文字等价说明：后台或接口对权威源库执行写操作并判定事件类型，随后发送 MQ 消息；消费者按 doc\_id 顺序消费消息，分别更新本地BM25索引与Qdrant向量库；检索阶段两路召回结果按 RRF 融合。每日/每周定时任务从权威源全量重建双索引作为兜底。

## 三、权威源设计

### 1\. 数据表结构

```sql
CREATE TABLE kb_doc (
  id           bigint        NOT NULL AUTO_INCREMENT   COMMENT '主键ID',
  doc_id       VARCHAR(64)   NOT NULL                  COMMENT '文档ID',
  doc_group    VARCHAR(32)   NOT NULL                  COMMENT '文档分组 可选分组：cardio_health / resp_health / ped_health / endo_health / women_health / common_living',
  title        VARCHAR(512)  NOT NULL                  COMMENT '文档标题',
  content      LONGTEXT      NOT NULL                  COMMENT '文档正文（或正文分块）',
  chunk_id     INT           NOT NULL DEFAULT 0        COMMENT '分块序号，不分块恒为 0',
  content_md5  CHAR(32)      NOT NULL                  COMMENT '正文 MD5，变更判定依据',
  status       TINYINT       NOT NULL DEFAULT 1        COMMENT '1 有效 / 0 已删除（软删除）',
  gmt_create   DATETIME      DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  gmt_update   DATETIME      DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (id),
  KEY idx_doc_id (doc_id),
  KEY idx_group_status (doc_group, status),
  KEY idx_status_ts (status, gmt_update)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='知识库文档';
```

### 2\. 分组、主键与分块规范

- **doc\_id**：文档首次上传时生成 UUID（推荐 v4），后续编辑、删除永久不变

- **doc\_group**：文档分组（上传时下拉必选），全链路贯穿，作为 Qdrant 查询前置过滤维度

- **chunk\_id**：长文档预切块（如 512 token / 50% 重叠），分块唯一 ID = `doc_id + "_" + 分块下标`

- **content\_md5**：按整篇文档计算，单块变化不触发全篇重索引

- **删除**：一律软删除（status=0），保留记录与 update\_ts，支撑恢复与全量跳过

### 3\. 文档分组枚举（上传必选，下拉选择）

|分组代码|分组名称|说明|
|---|---|---|
|`cardio_health`|心血管|心血管疾病相关文档|
|`resp_health`|呼吸|呼吸系统相关文档|
|`ped_health`|儿童健康|儿科与儿童健康相关文档|
|`endo_health`|内分泌|内分泌与代谢相关文档|
|`women_health`|女性健康|妇科与女性健康相关文档|
|`common_living`|通用居家健康|居家日常健康通用文档|

分组贯穿权威源、MQ 消息、BM25 doc\_ids、Qdrant payload 全链路；**检索时 Qdrant 先按 doc\_group 过滤，再在过滤后的集合内执行向量查询**，缩小检索空间、提升精度与性能。

## 四、事件链路设计

### 1\. MQ 主题与消息体

- 统一主题：`rag_doc_change_topic`

- 消息 Tag：`TAG_ADD` / `TAG_UPDATE` / `TAG_DELETE`，便于消费者过滤

- 分区键：**doc\_id（UUID）**，保证同一文档事件顺序消费

```json
{
  "doc_id": "uuid-xxxxxxxx",
  "doc_group": "cardio_health",
  "title": "文档标题",
  "content": "文档分块正文",
  "chunk_id": 0,
  "content_md5": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "update_ts": "2026-10-01 12:00:00",
  "event_type": "ADD",
  "msg_id": "全局唯一消息ID（幂等去重）"
}
```

### 2\. 生产端事件判定（唯一出口）

所有文档操作统一**先落库、后判类型、再发消息**，业务侧无权指定事件类型：

1. 数据库无该 UUID 记录 → 插入数据 → 发送 ADD

2. 数据库存在该 UUID，且 content\_md5 变化 → 更新数据 → 发送 UPDATE；md5 一致则不发消息

3. 数据库存在该 UUID，执行删除 → 软删除（status=0）→ 发送 DELETE

### 3\. 消费端处理规则

- **幂等**：msg\_id \+ doc\_id 双重去重，重试/重发不产生重复索引与重复向量

- **顺序**：同一 doc\_id 按消息顺序消费，防止先删后增、旧消息覆盖新内容

- **重试**：消费失败自动重试 3 次，最终投递死信队列，不丢失变更

## 五、双索引更新设计

### 1\. 新增（ADD）

消费者二次校验索引中不存在该 doc\_id 后：文本分词 → bm25s 增量追加 → 生成 Embedding → Qdrant Upsert → BM25 索引持久化。

### 2\. 修改（UPDATE）

校验索引存在且 md5 不一致后：旧 doc\_id 加入逻辑删除集合（检索自动过滤）→ 新内容重新分词并增量追加 → Qdrant 按 doc\_id 删旧向量、写新向量 → 更新本地 md5 缓存。

### 3\. 删除（DELETE）

doc\_id 加入逻辑删除集合 → Qdrant 按 doc\_id 删除向量 → BM25 索引本体不动，等待每日全量重建统一清理。

### 4\. 增量限制

单次增量同步仅允许小批量追加（如单次最多 200 条）；Qdrant 高频变更做消息批量聚合，减少删插次数、降低 HNSW 碎片。

### 5\. 分组维度检索（Qdrant 先过滤后向量查询）

检索链路按**分组前置过滤**设计，保证跨分组不串扰、检索空间最小化：

1. 检索请求携带目标分组 doc\_group（如 cardio\_health）

2. **Qdrant**：先执行 payload 过滤 `doc_group == cardio_health`，再在过滤结果集内执行向量近邻查询（HNSW 过滤检索），返回该分组 TopK 候选

3. **本地BM25**：分词检索后按 doc\_group 过滤命中文档（或按分组建分片索引），返回该分组 TopK 候选

4. 两路结果按 doc\_id 对齐后 RRF 融合，输出最终召回段落

```python
from qdrant_client import QdrantClient, models

client = QdrantClient("http://localhost:6333")
results = client.query_points(
    collection_name="kb_prod",
    query=query_vector,                      # 目标分组文档的语义向量
    query_filter=models.Filter(
        must=[models.FieldCondition(
            key="doc_group",
            match=models.MatchValue(value="cardio_health")
        )]
    ),
    limit=150,
)
```

**注意**：Qdrant 的 filtered HNSW 查询先缩小候选集再做图遍历，分组过滤能显著降低无效遍历；分组字段必须同步写入权威源、MQ 消息与 Qdrant payload。

## 六、兜底重建机制

1. **每日凌晨：本地BM25全量重建**——全量拉取权威源有效文档，重新分词构建索引，原子替换内存引用（零停机），清空逻辑删除集合、重算全局 IDF，修复漏消费与乱序造成的索引陈旧

2. **每周低峰：Qdrant双集合切换**——新建 kb\_v2 全量写入，校验通过后切换别名 kb\_prod 指向 v2，删除旧集合，彻底清除 HNSW 碎片与脏数据

## 七、后台管理页面设计

管理后台提供**权威源文档列表页**作为全部变更的操作入口，页面只写权威源库，不直接操作索引。

<!-- ![示例图片](./lists.png "文档列表") -->

### 2\. 上传文档弹窗设计（分组下拉选择）

上传弹窗在文件选择后要求**必选文档分组**（下拉选择六分组之一），确认后由服务端生成 UUID、自动分块并发送 ADD 消息；分组决定后续检索过滤范围。

<!-- ![示例图片](./add.png "文档列表") -->

页面职责说明：文档列表分页展示权威源记录，**列表含分组列**（cardio\_health / resp\_health / ped\_health / endo\_health / women\_health / common\_living），支持按分组与状态筛选、按标题搜索；**上传**时通过**分组下拉选择器**必选 doc\_group 并生成 UUID 落库，**编辑**修改内容与分组并重算 md5，**删除**软删除（status=0），**恢复**将软删文档重置为有效并重发 ADD。所有操作仅写权威源，索引变更由 MQ 异步驱动。

## 八、数据一致性保障

- **单入口写源**：所有变更统一由后台/接口写权威源并触发 MQ，无多入口直接写索引

- **主键贯穿**：UUID doc\_id 贯穿权威源、MQ 消息、BM25 doc\_ids、逻辑删除集合、Qdrant Point，全链路唯一

- **幂等消费**：msg\_id \+ doc\_id 去重，重复投递不产生冗余数据

- **双兜底**：每日 BM25 全量重建 \+ 每周 Qdrant 切换，收敛消息丢失、乱序与索引碎片

## 九、落地验收清单

* [ ] 权威源 kb\_doc 表上线，UUID 主键与软删除生效

* [ ] RocketMQ 主题、Tag、顺序消费、幂等、死信机制配置完成

* [ ] 消费者覆盖 ADD / UPDATE / DELETE 三种事件，双索引更新验证通过

* [ ] 管理后台列表页完成上传、编辑、删除、恢复、搜索、分页

* [ ] 每日 BM25 全量重建与每周 Qdrant 双集合切换定时任务上线

* [ ] 端到端演练：上传→秒级可检索、编辑→秒级更新、删除→秒级不可检索

* [ ] 分组字段全链路生效：权威源、MQ 消息、BM25、Qdrant payload 均携带 doc\_group

* [ ] Qdrant 查询先按 doc\_group 过滤再向量检索，验证跨分组不串扰

* [ ] 上传弹窗含分组下拉选择（六分组必选），文档列表显示分组列

> （注：部分内容由豆包工作 AI 生成）

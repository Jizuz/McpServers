package com.jizuz.mcpserver.dao;

import com.jizuz.mcpserver.dao.Entity.KbDoc;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 权威源kb_doc表访问层（每行=一个文档分块，文档级操作按doc_id聚合）
 */
@Repository
@RequiredArgsConstructor
public class KbDocDao {

    private final JdbcTemplate jdbcTemplate;

    private static final RowMapper<KbDoc> KB_DOC_MAPPER = (rs, rowNum) -> mapRow(rs);

    private static KbDoc mapRow(ResultSet rs) throws SQLException {
        return KbDoc.builder()
                .id(rs.getLong("id"))
                .docId(rs.getString("doc_id"))
                .docGroup(rs.getString("doc_group"))
                .title(rs.getString("title"))
                .content(rs.getString("content"))
                .chunkId(rs.getInt("chunk_id"))
                .contentMd5(rs.getString("content_md5"))
                .status(rs.getInt("status"))
                .gmtCreate(rs.getObject("gmt_create", LocalDateTime.class))
                .gmtUpdate(rs.getObject("gmt_update", LocalDateTime.class))
                .build();
    }

    /**
     * 文档级分页列表（行聚合为文档，支持分组/状态筛选与标题搜索）
     */
    public List<Map<String, Object>> pageDocs(String docGroup, Integer status, String keyword, int page, int size) {
        StringBuilder sql = new StringBuilder(
                "SELECT doc_id, MAX(doc_group) AS doc_group, MAX(title) AS title, MIN(status) AS status, " +
                        "MAX(gmt_update) AS gmt_update, COUNT(*) AS chunks FROM kb_doc WHERE 1=1");
        List<Object> args = new ArrayList<>();
        appendConditions(sql, args, docGroup, status, keyword);
        sql.append(" GROUP BY doc_id ORDER BY MAX(gmt_update) DESC LIMIT ?, ?");
        args.add(Math.max(0, (page - 1) * size));
        args.add(size);
        return jdbcTemplate.queryForList(sql.toString(), args.toArray());
    }

    /**
     * 文档总数（同筛选条件）
     */
    public long countDocs(String docGroup, Integer status, String keyword) {
        StringBuilder sql = new StringBuilder("SELECT COUNT(DISTINCT doc_id) FROM kb_doc WHERE 1=1");
        List<Object> args = new ArrayList<>();
        appendConditions(sql, args, docGroup, status, keyword);
        Long count = jdbcTemplate.queryForObject(sql.toString(), Long.class, args.toArray());
        return count == null ? 0 : count;
    }

    private void appendConditions(StringBuilder sql, List<Object> args, String docGroup, Integer status, String keyword) {
        if (docGroup != null && !docGroup.isBlank()) {
            sql.append(" AND doc_group = ?");
            args.add(docGroup);
        }
        if (status != null) {
            sql.append(" AND status = ?");
            args.add(status);
        }
        if (keyword != null && !keyword.isBlank()) {
            sql.append(" AND title LIKE ?");
            args.add("%" + keyword.trim() + "%");
        }
    }

    /**
     * 按docId查全部有效块（chunk_id升序，编辑回显拼全篇/恢复取内容）
     */
    public List<KbDoc> findValidChunks(String docId) {
        return jdbcTemplate.query(
                "SELECT * FROM kb_doc WHERE doc_id = ? AND status = 1 ORDER BY chunk_id",
                KB_DOC_MAPPER, docId);
    }

    /**
     * 按docId查任意状态块（含软删行，删除/恢复前校验用）
     */
    public List<KbDoc> findAnyChunks(String docId) {
        return jdbcTemplate.query(
                "SELECT * FROM kb_doc WHERE doc_id = ? ORDER BY chunk_id",
                KB_DOC_MAPPER, docId);
    }

    /**
     * 查文档当前MD5（同文档所有块相同），无有效记录返回null
     */
    public String findDocMd5(String docId) {
        List<String> md5s = jdbcTemplate.queryForList(
                "SELECT content_md5 FROM kb_doc WHERE doc_id = ? AND status = 1 LIMIT 1",
                String.class, docId);
        return md5s.isEmpty() ? null : md5s.get(0);
    }

    /**
     * 批量插入块行
     */
    public void insertChunks(List<KbDoc> chunks) {
        jdbcTemplate.batchUpdate(
                "INSERT INTO kb_doc(doc_id, doc_group, title, content, chunk_id, content_md5, status) VALUES(?, ?, ?, ?, ?, ?, ?)",
                chunks, 500,
                (ps, doc) -> {
                    ps.setString(1, doc.getDocId());
                    ps.setString(2, doc.getDocGroup());
                    ps.setString(3, doc.getTitle());
                    ps.setString(4, doc.getContent());
                    ps.setInt(5, doc.getChunkId());
                    ps.setString(6, doc.getContentMd5());
                    ps.setInt(7, doc.getStatus() == null ? 1 : doc.getStatus());
                });
    }

    /**
     * 物理删除该文档全部行（编辑替换旧行场景：新分块数可能与旧不同）
     */
    public void deleteRowsByDocId(String docId) {
        jdbcTemplate.update("DELETE FROM kb_doc WHERE doc_id = ?", docId);
    }

    /**
     * 软删除（status=0），保留记录支撑恢复与全量重建跳过
     */
    public void softDeleteByDocId(String docId) {
        jdbcTemplate.update("UPDATE kb_doc SET status = 0 WHERE doc_id = ?", docId);
    }

    /**
     * 恢复软删文档（status重置为1）
     */
    public void restoreByDocId(String docId) {
        jdbcTemplate.update("UPDATE kb_doc SET status = 1 WHERE doc_id = ?", docId);
    }

    /**
     * 全部有效块（doc_id、chunk_id升序），供BM25/Qdrant全量重建
     */
    public List<KbDoc> listValidChunks() {
        return jdbcTemplate.query(
                "SELECT * FROM kb_doc WHERE status = 1 ORDER BY doc_id, chunk_id",
                KB_DOC_MAPPER);
    }
}

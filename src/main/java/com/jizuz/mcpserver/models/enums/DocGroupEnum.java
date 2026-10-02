package com.jizuz.mcpserver.models.enums;

import java.util.Arrays;

/**
 * 知识库文档分组枚举（上传必选），
 * 分组贯穿权威源、MQ消息、BM25索引、Qdrant payload与检索过滤全链路
 */
public enum DocGroupEnum {

    CARDIO_HEALTH("cardio_health", "心血管"),
    RESP_HEALTH("resp_health", "呼吸"),
    PED_HEALTH("ped_health", "儿童健康"),
    ENDO_HEALTH("endo_health", "内分泌"),
    WOMEN_HEALTH("women_health", "女性健康"),
    COMMON_LIVING("common_living", "通用居家健康");

    private final String code;
    private final String name;

    DocGroupEnum(String code, String name) {
        this.code = code;
        this.name = name;
    }

    public String getCode() {
        return code;
    }

    public String getName() {
        return name;
    }

    /**
     * 分组代码是否合法（六分组之一）
     */
    public static boolean isValid(String code) {
        return code != null && Arrays.stream(values()).anyMatch(g -> g.code.equals(code));
    }

    /**
     * code转枚举，非法返回null
     */
    public static DocGroupEnum of(String code) {
        return Arrays.stream(values()).filter(g -> g.code.equals(code)).findFirst().orElse(null);
    }

    /**
     * code转中文名，非法返回原值
     */
    public static String nameOf(String code) {
        DocGroupEnum g = of(code);
        return g == null ? String.valueOf(code) : g.name;
    }
}

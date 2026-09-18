package com.jizuz.mcpserver.tools;

import com.alibaba.nacos.shaded.com.google.common.collect.Lists;
import io.modelcontextprotocol.spec.McpSchema;
import lombok.extern.slf4j.Slf4j;
import org.springaicommunity.mcp.annotation.McpTool;
import org.springaicommunity.mcp.annotation.McpToolParam;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class LocationMapTool {

    private record Store(String storeName, String transport, String distance) {}

    @McpTool(name = "get_nearby_stores")
    public McpSchema.CallToolResult getNearbyStores(@McpToolParam String location) {
        Store store1 = new Store("拜尔齿科五角场店", "地铁10号线五角场站", "距离您10km");
        Store store2 = new Store("瑞尔齿科三门路店", "地铁10号线三门路站", "距离您8km");
        return McpSchema.CallToolResult
                .builder()
                .addTextContent("查询附近门店成功")
                .structuredContent(Lists.newArrayList(store1, store2))
                .build();
    }

}

package com.jizuz.mcpserver.tools;

import com.alibaba.nacos.shaded.com.google.common.collect.Lists;
import com.jizuz.mcpserver.models.AppointReq;
import io.modelcontextprotocol.spec.McpSchema;
import lombok.extern.slf4j.Slf4j;
import org.springaicommunity.mcp.annotation.McpTool;
import org.springaicommunity.mcp.annotation.McpToolParam;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class RightTool {

    public record UserRightDTO(Long userId, String rightName, Long rightId, String expireDate) {}

    @McpTool(name = "get_right_list")
    public McpSchema.CallToolResult getRightList(@McpToolParam Long userId) {
        UserRightDTO dto1 = new UserRightDTO(10001002001L, "洁牙一次卡", 1000001001L, "2026-09-08");
        UserRightDTO dto2 = new UserRightDTO(10001002001L, "洁牙半年卡", 1000001005L, "2026-12-08");
        return McpSchema.CallToolResult
                .builder()
                .addTextContent("用户权益列表查询成功")
                .structuredContent(Lists.newArrayList(dto1, dto2))
                .build();
    }

    @McpTool(name = "valid_right")
    public McpSchema.CallToolResult validRight(@McpToolParam Long rightId) {
        // 假设有库存校验逻辑
        return McpSchema.CallToolResult
                .builder()
                .addTextContent("权益校验通过，可以使用")
                .structuredContent(true)
                .build();
    }

    @McpTool(name = "appoint_service")
    public McpSchema.CallToolResult appoint(@McpToolParam AppointReq req) {
        System.out.println(req.userName() + "/" + req.rightId() + "/" + req.mobile() + "/" + req.appointDatetime());
        return McpSchema.CallToolResult
                .builder()
                .addTextContent("预约成功")
                .structuredContent(true)
                .build();
    }

}

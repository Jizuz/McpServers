package com.jizuz.mcpserver;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.text.SimpleDateFormat;
import java.util.Date;

@SpringBootApplication
@EnableDiscoveryClient
@EnableScheduling
public class McpToolApplication {

    public static void main(String[] args) {
        SpringApplication.run(McpToolApplication.class, args);
        System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + " MCP server started!");
    }

}

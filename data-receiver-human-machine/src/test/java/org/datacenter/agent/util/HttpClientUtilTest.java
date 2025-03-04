package org.datacenter.agent.util;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

/**
 * @author : [wangminan]
 * @description : 测试HttpClient的一些功能
 */
public class HttpClientUtilTest {

    @Test
    public void testMain() throws URISyntaxException, IOException, InterruptedException {
        HttpClient client = HttpClient.newHttpClient();

        HttpRequest request = HttpRequest.newBuilder()
                .headers("Cookie")
                .uri(new URI("http://localhost:8080/api/v1/users"))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        // 获取cookie
        response.headers().map().forEach((k, v) -> {
            if (k != null && k.equals("Set-Cookie")) {
                System.out.println("Cookie: " + v);
            }
        });


        System.out.println(response.body());
    }
}

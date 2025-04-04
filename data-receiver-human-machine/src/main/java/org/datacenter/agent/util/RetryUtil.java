package org.datacenter.agent.util;

import lombok.extern.slf4j.Slf4j;
import org.datacenter.exception.ZorathosException;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.function.Supplier;

/**
 * @author : [wangminan]
 * @description : 重试工具
 */
@Slf4j
public class RetryUtil {

    /**
     * 执行带有重试机制的 HTTP 请求
     * @param requestSupplier HTTP 请求提供者
     * @param client HTTP 客户端
     * @param maxRetries 最大重试次数
     * @param httpResponseHandler 响应处理器
     * @param <T> 返回类型
     * @return 处理后的响应结果
     */
    public static <T> T executeHttpRequestWithRetry(
            Supplier<HttpRequest> requestSupplier,
            HttpClient client,
            int maxRetries,
            HttpResponseHandler<T> httpResponseHandler) {

        int retries = 0;
        Exception lastException = null;

        while (retries <= maxRetries) {
            try {
                HttpRequest request = requestSupplier.get();
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() == 200) {
                    return httpResponseHandler.handle(response);
                } else {
                    log.warn("Request failed with status code: {}, body: {}, retry: {}/{}",
                            response.statusCode(), response.body(), retries, maxRetries);
                }
            } catch (Exception e) {
                lastException = e;
                log.warn("Request failed with exception, retry: {}/{}", retries, maxRetries, e);
            }

            retries++;
            if (retries <= maxRetries) {
                try {
                    // 指数退避策略
                    Thread.sleep((long) Math.pow(2, retries) * 1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new ZorathosException(ie, "Retry interrupted");
                }
            }
        }

        throw new ZorathosException(lastException, "Failed after " + maxRetries + " retries");
    }

    /**
     * 响应处理接口
     */
    public interface HttpResponseHandler<T> {
        T handle(HttpResponse<String> response) throws Exception;
    }
}

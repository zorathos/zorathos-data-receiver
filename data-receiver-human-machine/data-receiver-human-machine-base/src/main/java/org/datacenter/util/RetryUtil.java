package org.datacenter.util;

import lombok.extern.slf4j.Slf4j;
import org.datacenter.exception.ZorathosException;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.function.Supplier;

@Slf4j
public class RetryUtil {

    public static <T> T executeHttpRequestWithRetry(
            Supplier<HttpRequest> requestSupplier,
            HttpClient client,
            int maxRetries,
            HttpResponseHandler<T> httpResponseHandler) {

        return executeWithRetry(() -> {
            try {
                HttpRequest request = requestSupplier.get();
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 200) {
                    return httpResponseHandler.handle(response);
                } else {
                    log.warn("Request failed with status code: {}, body: {}", response.statusCode(), response.body());
                    throw new ZorathosException("Request failed with status code: " + response.statusCode());
                }
            } catch (Exception e) {
                throw new ZorathosException(e, "HTTP request failed");
            }
        }, maxRetries, "HTTP Request");
    }

    public static <T> T executeWithRetry(
            Supplier<T> operation,
            int maxRetries,
            String operationName) {

        int retries = 0;
        Exception lastException = null;

        while (retries <= maxRetries) {
            try {
                return operation.get();
            } catch (Exception e) {
                lastException = e;
                log.warn("Operation '{}' failed, retry: {}/{}", operationName, retries, maxRetries, e);
            }

            // 指数退避（Exponential Backoff）重试策略
            retries++;
            if (retries <= maxRetries) {
                try {
                    Thread.sleep((long) Math.pow(2, retries) * 1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new ZorathosException(ie, "Retry interrupted");
                }
            }
        }

        if (lastException != null) {
            throw new ZorathosException(lastException, "Operation '" + operationName + "' failed after " + maxRetries + " retries");
        }
        return null;
    }

    public interface HttpResponseHandler<T> {
        T handle(HttpResponse<String> response) throws Exception;
    }
}

package com.reactivejava.sec05.assignment;

import com.reactivejava.common.AbstractHttpClient;
import reactor.core.publisher.Mono;

import java.time.Duration;

public class ExternalServiceClient extends AbstractHttpClient {

    public Mono<String> getProductName(int productId) {
        var actualPath = "/demo03/product/" + productId;
        var fallbackPathForTimeout = "/demo03/timeout-fallback/product/" + productId;
        var fallbackPathForEmpty = "/demo03/empty-fallback/product/" + productId;

        return getProductName(actualPath)
                .timeout(Duration.ofSeconds(2), getProductName(fallbackPathForTimeout))
                .switchIfEmpty(getProductName(fallbackPathForEmpty));
    }

    private Mono<String> getProductName(String path) {
        return this.httpClient.get()
                .uri(path)
                .responseContent()
                .asString()
                .next();
    }

}

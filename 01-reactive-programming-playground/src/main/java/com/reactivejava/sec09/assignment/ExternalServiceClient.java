package com.reactivejava.sec09.assignment;

import com.reactivejava.common.AbstractHttpClient;
import reactor.core.publisher.Mono;

// just for demo
public class ExternalServiceClient extends AbstractHttpClient {

    public Mono<Product> getProduct(int id) {
        return Mono.zip(getProductName(id), getProductReview(id), getProductPrice(id))
                .map(t -> new Product(t.getT1(), t.getT2(), t.getT3()));
    }

    private Mono<String> getProductName(int productId) {
        return get("/demo05/product/" + productId);
    }

    private Mono<String> getProductReview(int productId) {
        return get("/demo05/review/" + productId);
    }

    private Mono<String> getProductPrice(int productId) {
        return get("/demo05/price/" + productId);
    }

    private Mono<String> get(String path) {

        return this.httpClient.get()
                .uri(path)
                .responseContent()
                .asString()
                .next();
    }

}

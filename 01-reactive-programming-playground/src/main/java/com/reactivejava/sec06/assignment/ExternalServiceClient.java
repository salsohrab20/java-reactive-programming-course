package com.reactivejava.sec06.assignment;

import com.reactivejava.common.AbstractHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.util.Objects;


public class ExternalServiceClient extends AbstractHttpClient {

    private static final Logger logger = LoggerFactory.getLogger(ExternalServiceClient.class);
    private Flux<Order> orderFlux;

    public Flux<Order> orderStream() {
        if(Objects.isNull(orderFlux)) {
            this.orderFlux = getOrderStream();
        }
        return orderFlux;
    }


    private Flux<Order> getOrderStream() {
        return this.httpClient.get()
                .uri("/demo04/orders/stream")
                .responseContent()
                .asString()
                .map(this::parseOrder)
                .doOnNext(o -> logger.info("{}", o))
                .publish()
                .refCount(2);
    }

    private Order parseOrder(String message) {
        var arr = message.split(":");
        return new Order(arr[1],
                Integer.parseInt(arr[2]),
                Integer.parseInt(arr[3]));

    }
}

package com.reactivejava.sec02;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

/*
    To discuss some of the subscribe overloaded methods
 */
public class Lec03MonoSubscribe {

    private static final Logger log = LoggerFactory.getLogger(Lec03MonoSubscribe.class);

    public static void main(String[] args) {

        Mono<Integer> just = Mono.just(1);

        just.subscribe(
                s-> log.info("received : {}",s),
                err -> log.error("Error", err),
                ()-> log.info("Completed"),
                subscription ->  subscription.request(1)
        );

    }

}

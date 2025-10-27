package com.reactivejava.sec09.helper;

import com.reactivejava.common.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;

public class NameGenerator {

    private static Logger log = LoggerFactory.getLogger(NameGenerator.class);
    private final List<String> redisCache = new ArrayList<>();


    public Flux<String> generateOrders(){
        return Flux.generate(
                sink -> {
                    log.info("generating orders");
                    Util.sleepSeconds(1);
                    String productName = Util.faker().commerce().productName();
                    redisCache.add(productName);
                    sink.next(productName);
                }
        )
                .startWith(redisCache)
                .cast(String.class);
    }


    public Flux<String> generateNames() {
        return Flux.generate(fluxSink -> {
            log.info("generating names");
            Util.sleepSeconds(1);
            String name = Util.faker().name().firstName();
            redisCache.add(name);
            fluxSink.next(name);
        })
                .startWith(redisCache)
                .cast(String.class);
    }


}

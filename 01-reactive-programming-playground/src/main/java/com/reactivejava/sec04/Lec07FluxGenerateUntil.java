package com.reactivejava.sec04;

import com.reactivejava.common.Util;
import reactor.core.publisher.Flux;

public class Lec07FluxGenerateUntil {

    public static void main(String[] args) {

        demo3WithGenerate();

    }

    private static void demo1WithCreate() {
        Flux.create(
                fluxSink -> {
                    String country;
                    do{
                        country= Util.faker().country().name();
                        fluxSink.next(country);
                    }
                    while(!country.equalsIgnoreCase("canada"));
                    fluxSink.complete();
                }
        ).subscribe(System.out::println);
    }

    private static void demo2WithGenerate() {
        Flux.generate(
                synchronousSink -> {
                    var country = Util.faker().country().name();
                    synchronousSink.next(country);
                    if(country.equalsIgnoreCase("canada")) {
                        synchronousSink.complete();
                    }
                }
        ).subscribe(Util.subscriber());
    }

    private static void demo3WithGenerate() {
        Flux.<String>generate(
                synchronousSink -> {
                    var country = Util.faker().country().name();
                    synchronousSink.next(country);
                }

        ).takeUntil(c-> c.equalsIgnoreCase("canada")).subscribe(Util.subscriber());
    }


}

package com.reactivejava.sec03;

import com.reactivejava.common.Util;
import reactor.core.publisher.Flux;

public class Lec06Log {

    public static void main(String[] args) {


        Flux.range(1, 5)
                .log("range-map")
                .take(2)
                .map(i -> Util.faker().name().firstName())
                .log("map-range")
                .subscribe(Util.subscriber());


    }

}

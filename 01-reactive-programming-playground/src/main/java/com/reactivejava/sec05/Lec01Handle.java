package com.reactivejava.sec05;

import com.reactivejava.common.Util;
import reactor.core.publisher.Flux;

/*
    Handle behaves like filter + map

    1 => -2
    4 => do not send
    7 => error
    everything else => send as it is
*/
public class Lec01Handle {

    public static void main(String[] args) {

        Flux.range(1,100)
                .take(25)
                //.log()
                .takeWhile(i-> i<10)
                //.log()
               .takeUntil(i -> i>1 && i<5)
               // .log()
                .take(3)
                .log()
                .subscribe(Util.subscriber());

//        Flux.range(1, 10)
//                // .filter(i -> i != 7)
//                .handle((item, sink) -> {
//                    switch (item) {
//                        case 1 -> sink.next(-2);
//                        case 4 -> {
//                        }
//                        case 7 -> sink.error(new RuntimeException("7 error"));
//                        default -> sink.next(item);
//                    }
//                })
//                .cast(Integer.class)
//                .subscribe(Util.subscriber());


//        Flux.generate(synchronousSink -> synchronousSink.next(Util.faker().country().name())).handle((item, sink) -> {
//            sink.next(item);
//            if (item.toString().equalsIgnoreCase("canada")) {
//                sink.complete();
//            }
//        }).subscribe(Util.subscriber());


    }

}

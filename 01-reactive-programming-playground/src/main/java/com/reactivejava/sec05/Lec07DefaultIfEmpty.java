package com.reactivejava.sec05;

import reactor.core.publisher.Flux;

/*
    Similar to error handling.
    Handling empty!
 */
public class Lec07DefaultIfEmpty {

    public static void main(String[] args) {

//        Mono.fromSupplier(() -> Util.faker().name().toString())
//                .filter(name -> name.equalsIgnoreCase("canada"))
//                .defaultIfEmpty("No Canada")
//                        .subscribe(Util.subscriber());
//
//        Flux.range(1,10)
//                .filter(i-> i>10)
//                .defaultIfEmpty(100)
//                .subscribe(Util.subscriber());


       Flux.range(1,10)
               .filter(i-> i>5)
               .take(2)
               .next()
               .subscribe(System.out::println);
    }


}

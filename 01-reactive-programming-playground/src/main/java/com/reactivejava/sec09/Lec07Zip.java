package com.reactivejava.sec09;

import com.reactivejava.common.Util;
import reactor.core.publisher.Flux;

import java.time.Duration;

/*
    Zip
    - we will subscribe to all the producers at the same time
    - all or nothing
    - all producers will have to emit an item
 */
public class Lec07Zip {

    record Car(String body, String engine, String tyres) {}


    public static void main(String[] args) {
        Flux.zip(getBody(),getEngine(), getTyres())
                .map(t-> new Car(t.getT1(), t.getT2(),t.getT3()))
                .subscribe(Util.subscriber());

        Util.sleepSeconds(10);
    }

    private static Flux<String> getBody(){
        return Flux.range(1,5)
                .map(i-> "body - " + i)
                .delayElements(Duration.ofMillis(100));
    }

    private static Flux<String> getEngine(){
        return Flux.range(1,4)
                .map(i-> "engine - " + i)
                .delayElements(Duration.ofMillis(200));
    }

    private static Flux<String> getTyres(){
        return Flux.range(1,10)
                .map(i-> "tyres - " + i)
                .delayElements(Duration.ofMillis(75));
    }


}

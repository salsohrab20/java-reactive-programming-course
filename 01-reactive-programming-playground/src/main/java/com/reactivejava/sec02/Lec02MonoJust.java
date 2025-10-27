package com.reactivejava.sec02;

import com.reactivejava.sec01.subscriber.SubscriberImpl;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

/*
    Use just when the value to be emitted is already in the memory
 */

public class Lec02MonoJust {

    public static void main(String[] args) {

        Publisher<String> salman = Mono.just("Salman");
        var subscriber = new SubscriberImpl();
        salman.subscribe(subscriber);

        subscriber.getSubscription().request(10);
        subscriber.getSubscription().request(10);
        subscriber.getSubscription().cancel();
        subscriber.getSubscription().request(10);


    }


}

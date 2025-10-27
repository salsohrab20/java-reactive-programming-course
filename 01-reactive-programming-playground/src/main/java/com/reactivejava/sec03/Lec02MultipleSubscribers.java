package com.reactivejava.sec03;

import com.reactivejava.common.Util;
import reactor.core.publisher.Flux;

/*
    To demo filter / map operators
 */
public class Lec02MultipleSubscribers {


    public static void main(String[] args) {

        Flux<Integer> just = Flux.just(1, 2, 3, 4, 5);

        just.subscribe(Util.subscriber("sub1"));
        just.subscribe(Util.subscriber("sub2"));

        just.filter(i->i%2==0).map(a->a*2).subscribe(Util.subscriber("sub3"));

        just.filter(i-> i%3 ==0).map(k-> k*1000).subscribe(Util.subscriber("sub4"));

    }

}

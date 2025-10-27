package com.reactivejava.sec09;

import com.reactivejava.common.Util;
import com.reactivejava.sec09.assignment.ExternalServiceClient;
import reactor.core.publisher.Flux;

/*
    Ensure that the external service is up and running!
 */
public class Lec12FluxFlatMapAssignment {

    public static void main(String[] args) {

        var client = new ExternalServiceClient();

        Flux.range(1,10)
                        .flatMap(client::getProduct,3)
                                .subscribe(Util.subscriber());
        Util.sleepSeconds(20);

    }

}

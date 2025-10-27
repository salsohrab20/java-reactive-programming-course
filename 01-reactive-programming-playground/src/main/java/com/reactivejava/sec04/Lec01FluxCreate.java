package com.reactivejava.sec04;

import com.github.javafaker.Faker;
import reactor.core.publisher.Flux;

/*
    To create a flux & emit items programmatically
 */
public class Lec01FluxCreate {

    public static void main(String[] args) {
//        Flux<String> stringFlux = Flux.create(
//                fluxSink -> {
//                    fluxSink.next("a");
//                    fluxSink.next("b");
//                    fluxSink.complete();
//                }
//        );
//
//        stringFlux.subscribe(System.out::println);


//        Flux<String> fluxCountries = Flux.create(
//                countriesFlux -> {
//                    for(int i=0;i<10;i++)
//                    {
//                        countriesFlux.next(Faker.instance().country().name());
//                    }
//                    countriesFlux.complete();
//                }
//
//        );
//
//        fluxCountries.subscribe(Util.subscriber());


        Flux<String> canada = Flux.create(
                canadaFlux -> {
                    String country;
                    do {
                        country = Faker.instance().country().name();
                        canadaFlux.next(country);
                    } while (!country.equalsIgnoreCase("Canada"));

                    canadaFlux.complete();
                }
        );

        canada.subscribe(System.out::println);
    }

}

package com.reactivejava.sec09.applications;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;

public class UserService {

    private static final Map<String, Integer> userTable = Map.of(
            "Salman", 1,
            "Sohrab", 2,
            "Sadaf", 3
    );

    public static Flux<User> getAllUsers(){
        return Flux.fromIterable(userTable.entrySet())
                .map(entry -> new User(entry.getValue(), entry.getKey()));
    }

    public static Mono<Integer> getUserId(String name){
        return Mono.fromSupplier(()-> userTable.get(name));
    }

}

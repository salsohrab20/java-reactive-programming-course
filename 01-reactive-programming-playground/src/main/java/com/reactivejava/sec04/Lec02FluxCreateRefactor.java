package com.reactivejava.sec04;

import com.reactivejava.common.Util;
import com.reactivejava.sec04.helper.NameGenerator;
import reactor.core.publisher.Flux;

public class Lec02FluxCreateRefactor {

    public static void main(String[] args) {

        NameGenerator nameGenerator = new NameGenerator();
        var flux = Flux.create(nameGenerator);
        flux.subscribe(Util.subscriber());

        for(int i=0;i<10;i++){
            nameGenerator.generate();
        }

    }

}

package com.reactivejava.sec09;

import com.reactivejava.common.Util;
import com.reactivejava.sec09.helper.NameGenerator;

public class Lec02StartWithUseCase {

    public static void main(String[] args) {
        NameGenerator nameGenerator = new NameGenerator();

        nameGenerator.generateNames()
                .take(2)
                .subscribe(Util.subscriber("salman"));

        nameGenerator.generateNames()
                .take(2)
                .subscribe(Util.subscriber("sohrab"));

        nameGenerator.generateNames()
                .take(3)
                .subscribe(Util.subscriber("ansari"));


        nameGenerator.generateOrders()
                .take(2).subscribe(Util.subscriber("Sadaf"));

        nameGenerator.generateOrders()
                .take(2).subscribe(Util.subscriber("Akhtar"));

        nameGenerator.generateOrders()
                .take(3).subscribe(Util.subscriber("Liza"));

        nameGenerator.generateOrders()
                .take(3).subscribe(Util.subscriber("Sadaf"));
    }

}

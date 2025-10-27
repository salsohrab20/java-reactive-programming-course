package com.reactivejava.sec09;

import com.reactivejava.common.Util;
import com.reactivejava.sec09.applications.*;

/*
    Sequential non-blocking IO calls!
    flatMap is used to flatten the inner publisher / to subscribe to the inner publisher
 */
public class Lec09MonoFlatMap {

    public static void main(String[] args) {

        /*
            We have username.
            Get user account balance
         */

        UserService.getUserId("Salman")
                .flatMap(PaymentService::getUserBalance)
                .subscribe(Util.subscriber());

        Util.sleepSeconds(3);

    }

}

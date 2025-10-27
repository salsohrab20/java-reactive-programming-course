package com.reactivejava.sec09;

import com.reactivejava.common.Util;
import com.reactivejava.sec09.assignment.ExternalServiceClient;

/*
    Ensure that the external service is up and running!
 */
public class Lec08ZipAssignment {

    public static void main(String[] args) {

        var client = new ExternalServiceClient();

        for (int i = 1; i < 10; i++) {
            client.getProduct(i)
                    .subscribe(Util.subscriber());
        }

        Util.sleepSeconds(2);

    }

}

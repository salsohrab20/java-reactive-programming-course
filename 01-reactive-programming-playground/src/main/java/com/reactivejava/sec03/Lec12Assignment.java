package com.reactivejava.sec03;


import com.reactivejava.common.Util;
import com.reactivejava.sec03.assignment.StockPriceObserver;
import com.reactivejava.sec03.client.ExternalServiceClient;

/*
    Ensure that the external service is up and running!
 */
public class Lec12Assignment {

    public static void main(String[] args) {

        var client = new ExternalServiceClient();
        var subcriber = new StockPriceObserver();
        client.getPriceUpdates()
                .subscribe(subcriber);

        Util.sleepSeconds(20);
    }

}

package com.reactivejava.sec03.assignment;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StockPriceObserver implements Subscriber<Integer> {

    private static Logger log = LoggerFactory.getLogger(StockPriceObserver.class);
    private Subscription subscription;
    int balance = 1000;
    int quantity = 0;

    @Override
    public void onSubscribe(Subscription s) {
        this.subscription = s;
        this.subscription.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(Integer s) {

        if(s < 90 && balance >=s)
        {
            log.info("Price lower than 90, Buying stock , Before purchase quantity :{}, balance:{}", quantity, balance);
            quantity++;
            balance = balance - s;
            log.info("Purchased a stock , After purchase quantity :{}, balance:{}", quantity, balance);
        }

        else if(s > 110 && quantity > 0)
        {
            log.info("Price higher than 110, selling stock, before sale value, quantity : {}, balance:{}", quantity, balance);
            balance = balance + s*quantity;
            quantity=0;
            this.subscription.cancel();
            log.info("Sold stock, After sale value, quantity : {}, balance:{}", quantity, balance);
            log.info("Profit : {}", (balance -1000));
        }
    }

    @Override
    public void onError(Throwable t) {
        log.error(t.getMessage(), t);
    }

    @Override
    public void onComplete() {
        log.info("Operation completed");
    }
}

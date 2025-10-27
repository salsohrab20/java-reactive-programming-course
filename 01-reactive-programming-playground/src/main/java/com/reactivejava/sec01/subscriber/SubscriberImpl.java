package com.reactivejava.sec01.subscriber;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class SubscriberImpl implements Subscriber<String> {

    private static final Logger log = LoggerFactory.getLogger(SubscriberImpl.class);

    private Subscription subscription;

    public Subscription getSubscription() {
        return subscription;
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        this.subscription=subscription;
    }

    @Override
    public void onNext(String s) {
        log.info("onNext: " + s);
    }

    @Override
    public void onError(Throwable t) {
        log.error("onError: " + t);
    }

    @Override
    public void onComplete() {
        log.info("onComplete");
    }
}

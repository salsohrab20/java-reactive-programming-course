package com.reactivejava.sec01.publisher;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

public class PublisherImpl implements Publisher<String> {

    @Override
    public void subscribe(Subscriber<? super String> s) {
        var subscription = new SubscriptionImpl(s);
        s.onSubscribe(subscription);
    }
}

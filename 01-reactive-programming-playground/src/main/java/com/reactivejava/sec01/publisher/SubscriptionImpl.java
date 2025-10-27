package com.reactivejava.sec01.publisher;

import com.github.javafaker.Faker;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;



public class SubscriptionImpl implements Subscription {

    private final Subscriber<? super String> subscriber;
    private Faker faker;
    private boolean isCancelled;
    int MAX_COUNT=10;
    int count=0;
    public SubscriptionImpl(Subscriber<? super String> subscriber) {
        this.subscriber = subscriber;
        this.faker = Faker.instance();
    }

    @Override
    public void request(long n) {
        if(isCancelled) return;

        if(n > MAX_COUNT){
            this.subscriber.onError(new RuntimeException("Requested item greater than max count"));
            this.isCancelled = true;
            return;
        }

        for(int i=0 ;i<n && count<MAX_COUNT; i++)
        {
            this.subscriber.onNext(this.faker.internet().emailAddress());
            count++;
        }

        if(count == MAX_COUNT)
        {
            this.subscriber.onComplete();
            this.isCancelled = true;
        }
    }

    @Override
    public void cancel() {
        isCancelled = true;
    }
}

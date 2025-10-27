package com.reactivejava.sec03;

import com.reactivejava.common.Util;
import com.reactivejava.sec03.client.ExternalServiceClient;

/*
    To demo non-blocking IO with streaming messages
    Ensure that the external service is up and running!
 */
public class Lec08NonBlockingStreamingMessages {

    public static void main(String[] args) {

        var externalServiceClient = new ExternalServiceClient();
        externalServiceClient.getNames().subscribe(Util.subscriber("sub1"));

        externalServiceClient.getNames().subscribe(Util.subscriber("sub2"));

        Util.sleepSeconds(6);

    }

}

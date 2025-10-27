package com.reactivejava.sec06;

import com.reactivejava.common.Util;
import com.reactivejava.sec06.assignment.ExternalServiceClient;
import com.reactivejava.sec06.assignment.InventoryService;
import com.reactivejava.sec06.assignment.RevenueService;

/*
    Ensure that the external service is up and running!
 */
public class Lec06Assignment {

    public static void main(String[] args) {

        var client = new ExternalServiceClient();
        var inventoryService = new InventoryService();
        var revenueService = new RevenueService();

        client.orderStream().subscribe(inventoryService::consume);
        client.orderStream().subscribe(revenueService::consume);

        inventoryService.stream()
                        .subscribe(Util.subscriber("inventory"));

        revenueService.stream()
                        .subscribe(Util.subscriber("revenue"));


        Util.sleepSeconds(30);

    }

}

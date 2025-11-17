package com.mycompany.opt.v1;

import java.time.LocalTime;

public class Depot {
    private String depotId;
    private double latitude;
    private double longitude;
    private double capacityStorage;
    private LocalTime openTime;
    private LocalTime closeTime;

    public Depot(String depotId, double latitude, double longitude, 
                double capacityStorage, LocalTime openTime, LocalTime closeTime) {
        this.depotId = depotId;
        this.latitude = latitude;
        this.longitude = longitude;
        this.capacityStorage = capacityStorage;
        this.openTime = openTime;
        this.closeTime = closeTime;
    }

    // Getters
    public String getDepotId() { return depotId; }
    public double getLatitude() { return latitude; }
    public double getLongitude() { return longitude; }
    public double getCapacityStorage() { return capacityStorage; }
    public LocalTime getOpenTime() { return openTime; }
    public LocalTime getCloseTime() { return closeTime; }

    @Override
    public String toString() {
        return depotId;
    }
}


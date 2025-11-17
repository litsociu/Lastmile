package com.mycompany.opt.v1;

import java.time.LocalTime;

public class Customer {
    private String customerId;
    private double latitude;
    private double longitude;
    private double orderWeight;
    private double orderVolume;
    private LocalTime timeWindowStart;
    private LocalTime timeWindowEnd;
    private int serviceTime; // minutes
    private int priorityLevel;
    private String deliveryType; // Home, Office, etc.
    private boolean returnFlag; // Whether customer requires return service
    private boolean visited;

    public Customer(String customerId, double latitude, double longitude, 
                   double orderWeight, double orderVolume,
                   LocalTime timeWindowStart, LocalTime timeWindowEnd,
                   int serviceTime, int priorityLevel, String deliveryType, boolean returnFlag) {
        this.customerId = customerId;
        this.latitude = latitude;
        this.longitude = longitude;
        this.orderWeight = orderWeight;
        this.orderVolume = orderVolume;
        this.timeWindowStart = timeWindowStart;
        this.timeWindowEnd = timeWindowEnd;
        this.serviceTime = serviceTime;
        this.priorityLevel = priorityLevel;
        this.deliveryType = deliveryType;
        this.returnFlag = returnFlag;
        this.visited = false;
    }

    // Getters and Setters
    public String getCustomerId() { return customerId; }
    public double getLatitude() { return latitude; }
    public double getLongitude() { return longitude; }
    public double getOrderWeight() { return orderWeight; }
    public double getOrderVolume() { return orderVolume; }
    public LocalTime getTimeWindowStart() { return timeWindowStart; }
    public LocalTime getTimeWindowEnd() { return timeWindowEnd; }
    public int getServiceTime() { return serviceTime; }
    public int getPriorityLevel() { return priorityLevel; }
    public String getDeliveryType() { return deliveryType; }
    public boolean isReturnFlag() { return returnFlag; }
    public boolean isVisited() { return visited; }
    public void setVisited(boolean visited) { this.visited = visited; }

    @Override
    public String toString() {
        return customerId;
    }
}


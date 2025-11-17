package com.mycompany.opt.v1;

public class Road {
    private String originNodeId;
    private String destinationNodeId;
    private double distanceKm;
    private double travelTimeMin;
    private String trafficLevel; // Low, Medium, High
    private String roadRestrictions; // Restrictions like "No Heavy Trucks"

    public Road(String originNodeId, String destinationNodeId, 
                double distanceKm, double travelTimeMin, String trafficLevel, String roadRestrictions) {
        this.originNodeId = originNodeId;
        this.destinationNodeId = destinationNodeId;
        this.distanceKm = distanceKm;
        this.travelTimeMin = travelTimeMin;
        this.trafficLevel = trafficLevel;
        this.roadRestrictions = roadRestrictions;
    }

    // Getters
    public String getOriginNodeId() { return originNodeId; }
    public String getDestinationNodeId() { return destinationNodeId; }
    public double getDistanceKm() { return distanceKm; }
    public double getTravelTimeMin() { return travelTimeMin; }
    public String getTrafficLevel() { return trafficLevel; }
    public String getRoadRestrictions() { return roadRestrictions; }
    
    // Check if vehicle can use this road
    public boolean canUseRoad(Vehicle vehicle) {
        if (roadRestrictions == null || roadRestrictions.isEmpty()) {
            return true;
        }
        // Check if vehicle type is restricted
        String vehicleType = vehicle.getVehicleType().toLowerCase();
        String restrictions = roadRestrictions.toLowerCase();
        
        if (restrictions.contains("heavy") && (vehicleType.contains("truck") || vehicleType.contains("van"))) {
            return false;
        }
        return true;
    }
    
    // Get adjusted travel time based on traffic
    public double getAdjustedTravelTime() {
        double multiplier = 1.0;
        if ("High".equalsIgnoreCase(trafficLevel)) {
            multiplier = 1.3; // 30% slower in high traffic
        } else if ("Medium".equalsIgnoreCase(trafficLevel)) {
            multiplier = 1.15; // 15% slower in medium traffic
        }
        return travelTimeMin * multiplier;
    }
}


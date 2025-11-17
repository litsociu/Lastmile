package com.mycompany.opt.v1;

public class Vehicle {
    private String vehicleId;
    private String vehicleType;
    private double capacityWeight;
    private double capacityVolume;
    private double fixedCost;
    private double variableCost;
    private double maxDistance;
    private int maxWorkingHours;
    private String startDepotId;
    private String endDepotId;

    public Vehicle(String vehicleId, String vehicleType,
                   double capacityWeight, double capacityVolume,
                   double fixedCost, double variableCost,
                   double maxDistance, int maxWorkingHours,
                   String startDepotId, String endDepotId) {
        this.vehicleId = vehicleId;
        this.vehicleType = vehicleType;
        this.capacityWeight = capacityWeight;
        this.capacityVolume = capacityVolume;
        this.fixedCost = fixedCost;
        this.variableCost = variableCost;
        this.maxDistance = maxDistance;
        this.maxWorkingHours = maxWorkingHours;
        this.startDepotId = startDepotId;
        this.endDepotId = endDepotId;
    }

    // Getters
    public String getVehicleId() { return vehicleId; }
    public String getVehicleType() { return vehicleType; }
    public double getCapacityWeight() { return capacityWeight; }
    public double getCapacityVolume() { return capacityVolume; }
    public double getFixedCost() { return fixedCost; }
    public double getVariableCost() { return variableCost; }
    public double getMaxDistance() { return maxDistance; }
    public int getMaxWorkingHours() { return maxWorkingHours; }
    public String getStartDepotId() { return startDepotId; }
    public String getEndDepotId() { return endDepotId; }

    @Override
    public String toString() {
        return vehicleId;
    }
}


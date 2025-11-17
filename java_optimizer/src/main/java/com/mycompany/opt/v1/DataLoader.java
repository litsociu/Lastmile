package com.mycompany.opt.v1;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class DataLoader {
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm");
    
    // Helper method to get column value, handling BOM in header
    private static String getColumnValue(CSVRecord record, String columnName) {
        // Try exact match first
        try {
            return record.get(columnName);
        } catch (IllegalArgumentException e) {
            // Try with BOM prefix (UTF-8 BOM: ﻿)
            try {
                return record.get("\uFEFF" + columnName);
            } catch (IllegalArgumentException e2) {
                // Try trimming all headers
                for (String header : record.getParser().getHeaderNames()) {
                    if (header.trim().equals(columnName) || header.replace("\uFEFF", "").equals(columnName)) {
                        return record.get(header);
                    }
                }
                throw new IllegalArgumentException("Column '" + columnName + "' not found. Available columns: " + record.getParser().getHeaderNames());
            }
        }
    }

    public static List<Customer> loadCustomers(String filePath) throws IOException {
        List<Customer> customers = new ArrayList<>();
        // Use InputStreamReader with UTF-8 to handle BOM
        try (InputStreamReader reader = new InputStreamReader(
                new FileInputStream(filePath), StandardCharsets.UTF_8);
             CSVParser csvParser = new CSVParser(reader, 
                CSVFormat.DEFAULT.withFirstRecordAsHeader().withTrim())) {
            
            for (CSVRecord record : csvParser) {
                String customerId = getColumnValue(record, "Customer_ID");
                double latitude = Double.parseDouble(getColumnValue(record, "Latitude"));
                double longitude = Double.parseDouble(getColumnValue(record, "Longitude"));
                double orderWeight = Double.parseDouble(getColumnValue(record, "Order_Weight"));
                double orderVolume = Double.parseDouble(getColumnValue(record, "Order_Volume"));
                
                String timeStartStr = getColumnValue(record, "Time_Window_Start");
                String timeEndStr = getColumnValue(record, "Time_Window_End");
                LocalTime timeWindowStart = LocalTime.parse(timeStartStr, TIME_FORMATTER);
                LocalTime timeWindowEnd = LocalTime.parse(timeEndStr, TIME_FORMATTER);
                
                int serviceTime = Integer.parseInt(getColumnValue(record, "Service_Time"));
                int priorityLevel = Integer.parseInt(getColumnValue(record, "Priority_Level"));
                String deliveryType = getColumnValue(record, "Delivery_Type");
                boolean returnFlag = Boolean.parseBoolean(getColumnValue(record, "Return_Flag"));
                
                customers.add(new Customer(customerId, latitude, longitude, 
                                         orderWeight, orderVolume,
                                         timeWindowStart, timeWindowEnd,
                                         serviceTime, priorityLevel, deliveryType, returnFlag));
            }
        }
        return customers;
    }

    public static List<Depot> loadDepots(String filePath) throws IOException {
        List<Depot> depots = new ArrayList<>();
        try (InputStreamReader reader = new InputStreamReader(
                new FileInputStream(filePath), StandardCharsets.UTF_8);
             CSVParser csvParser = new CSVParser(reader, 
                CSVFormat.DEFAULT.withFirstRecordAsHeader().withTrim())) {
            
            for (CSVRecord record : csvParser) {
                String depotId = getColumnValue(record, "Depot_ID");
                double latitude = Double.parseDouble(getColumnValue(record, "Latitude"));
                double longitude = Double.parseDouble(getColumnValue(record, "Longitude"));
                double capacityStorage = Double.parseDouble(getColumnValue(record, "Capacity_Storage"));
                
                String operatingHours = getColumnValue(record, "Operating_Hours");
                String[] times = operatingHours.split("-");
                LocalTime openTime = LocalTime.parse(times[0], TIME_FORMATTER);
                LocalTime closeTime = LocalTime.parse(times[1], TIME_FORMATTER);
                
                depots.add(new Depot(depotId, latitude, longitude, 
                                   capacityStorage, openTime, closeTime));
            }
        }
        return depots;
    }

    public static List<Vehicle> loadVehicles(String filePath) throws IOException {
        List<Vehicle> vehicles = new ArrayList<>();
        try (InputStreamReader reader = new InputStreamReader(
                new FileInputStream(filePath), StandardCharsets.UTF_8);
             CSVParser csvParser = new CSVParser(reader, 
                CSVFormat.DEFAULT.withFirstRecordAsHeader().withTrim())) {
            
            for (CSVRecord record : csvParser) {
                String vehicleId = getColumnValue(record, "Vehicle_ID");
                String vehicleType = getColumnValue(record, "Vehicle_Type");
                double capacityWeight = Double.parseDouble(getColumnValue(record, "Capacity_Weight"));
                double capacityVolume = Double.parseDouble(getColumnValue(record, "Capacity_Volume"));
                double fixedCost = Double.parseDouble(getColumnValue(record, "Fixed_Cost"));
                double variableCost = Double.parseDouble(getColumnValue(record, "Variable_Cost"));
                double maxDistance = Double.parseDouble(getColumnValue(record, "Max_Distance"));
                int maxWorkingHours = Integer.parseInt(getColumnValue(record, "Max_Working_Hours"));
                String startDepotId = getColumnValue(record, "Start_Depot_ID");
                String endDepotId = getColumnValue(record, "End_Depot_ID");
                
                vehicles.add(new Vehicle(vehicleId, vehicleType,
                                       capacityWeight, capacityVolume,
                                       fixedCost, variableCost,
                                       maxDistance, maxWorkingHours,
                                       startDepotId, endDepotId));
            }
        }
        return vehicles;
    }

    public static Map<String, Map<String, Road>> loadRoads(String filePath) throws IOException {
        Map<String, Map<String, Road>> roadMap = new HashMap<>();
        try (InputStreamReader reader = new InputStreamReader(
                new FileInputStream(filePath), StandardCharsets.UTF_8);
             CSVParser csvParser = new CSVParser(reader, 
                CSVFormat.DEFAULT.withFirstRecordAsHeader().withTrim())) {
            
            for (CSVRecord record : csvParser) {
                String originId = getColumnValue(record, "Origin_Node_ID");
                String destId = getColumnValue(record, "Destination_Node_ID");
                double distanceKm = Double.parseDouble(getColumnValue(record, "Distance_km"));
                double travelTimeMin = Double.parseDouble(getColumnValue(record, "Travel_Time_min"));
                String trafficLevel = getColumnValue(record, "Traffic_Level");
                String roadRestrictions = getColumnValue(record, "Road_Restrictions");
                
                Road road = new Road(originId, destId, distanceKm, travelTimeMin, trafficLevel, roadRestrictions);
                roadMap.computeIfAbsent(originId, k -> new HashMap<>()).put(destId, road);
            }
        }
        return roadMap;
    }

    public static String getDataPath(String cityName) {
        Path basePath = Paths.get("src", "main", "resources", "LMDO processed", cityName);
        return basePath.toString();
    }
}


package com.mycompany.opt.v1;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Main entry point for Cluster Leader Solver - Version 1
 * 
 * Usage: Run this in IntelliJ IDEA
 */
public class Main {
    
    public static void main(String[] args) {
        System.out.println("=".repeat(80));
        System.out.println("🗺️  CLUSTER LEADER SOLVER - Version 1");
        System.out.println("=".repeat(80));
        System.out.println();
        
        final String cityName = "Can_Tho";
        final String outputBaseDir = "java_optimizer/output";
        
        try {
            // Find data path
            String dataPath = findDataPath(cityName);
            if (dataPath == null) {
                System.err.println("❌ Không tìm thấy thư mục dữ liệu!");
                return;
            }
            
            System.out.println("📂 Đang đọc dữ liệu từ: " + dataPath);
            
            // Load customers
            String customersPath = dataPath + "/customers.csv";
            List<Customer> customers = DataLoader.loadCustomers(customersPath);
            System.out.println("✅ Đã đọc " + customers.size() + " khách hàng");
            
            // Load roads (optional)
            Map<String, Map<String, Road>> roadMap = new HashMap<>();
            try {
                String roadsPath = dataPath + "/roads.csv";
                roadMap = DataLoader.loadRoads(roadsPath);
                System.out.println("✅ Đã đọc " + roadMap.size() + " nodes trong road graph");
            } catch (Exception e) {
                System.out.println("⚠️  Không có road graph, sẽ dùng haversine distance");
            }
            
            // Get depot coordinates (use centroid if not specified)
            double depotLat = customers.stream().mapToDouble(Customer::getLatitude).average().orElse(0.0);
            double depotLon = customers.stream().mapToDouble(Customer::getLongitude).average().orElse(0.0);
            System.out.println("📍 Depot coordinates: (" + depotLat + ", " + depotLon + ")");
            
            // Create solver
            ClusterLeaderSolver solver = new ClusterLeaderSolver(customers, roadMap);
            
            // Run pipeline
            System.out.println("\n🚀 Bắt đầu clustering...");
            Map<String, Object> results = solver.runPipeline(
                2,      // P_min
                8,      // P_max
                1.0,    // alpha
                depotLat,
                depotLon,
                false,  // useGraph (Set to true if you have road graph)
                2000    // maxExactN
            );
            
            // Print results
            System.out.println("\n" + "=".repeat(80));
            System.out.println("📊 KẾT QUẢ CLUSTERING");
            System.out.println("=".repeat(80));
            
            @SuppressWarnings("unchecked")
            List<ClusterLeaderSolver.CustomerAssignment> assignments = 
                (List<ClusterLeaderSolver.CustomerAssignment>) results.get("assignments");
            
            System.out.println("Best P: " + results.get("bestP"));
            System.out.println("Best Objective: " + String.format("%.3f", (Double) results.get("bestObjective")));
            System.out.println("\nCustomer Assignments:");
            
            // Group by cluster
            Map<Integer, List<ClusterLeaderSolver.CustomerAssignment>> byCluster = new HashMap<>();
            for (ClusterLeaderSolver.CustomerAssignment assignment : assignments) {
                byCluster.computeIfAbsent(assignment.clusterId, k -> new ArrayList<>()).add(assignment);
            }
            
            for (Map.Entry<Integer, List<ClusterLeaderSolver.CustomerAssignment>> entry : byCluster.entrySet()) {
                System.out.println("\nCluster " + entry.getKey() + " (" + entry.getValue().size() + " customers):");
                for (ClusterLeaderSolver.CustomerAssignment a : entry.getValue()) {
                    System.out.println("  " + a);
                }
            }
            
            System.out.println("\n" + "=".repeat(80));
            System.out.println("✅ Hoàn thành!");
            System.out.println("=".repeat(80));
            saveResults(outputBaseDir, cityName, results);
            
            // Cleanup
            solver.shutdown();
            
        } catch (IOException e) {
            System.err.println("❌ Lỗi khi đọc dữ liệu: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("❌ Lỗi: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static String findDataPath(String cityName) {
        // Chỉ lấy dữ liệu từ đường dẫn này: java_optimizer/src/main/resources/LMDO processed/{cityName}/
        String dataPath = "java_optimizer/src/main/resources/LMDO processed/" + cityName;
        
        File dir = new File(dataPath);
        if (!dir.exists() || !dir.isDirectory()) {
            System.err.println("❌ Không tìm thấy thư mục: " + dataPath);
            System.err.println("   Đường dẫn đầy đủ: " + dir.getAbsolutePath());
            return null;
        }
        
        File customersFile = new File(dir, "customers.csv");
        if (!customersFile.exists()) {
            System.err.println("❌ Không tìm thấy file customers.csv trong: " + dataPath);
            return null;
        }
        
        System.out.println("✅ Đường dẫn dữ liệu: " + dataPath);
        System.out.println("   📁 Đường dẫn đầy đủ: " + dir.getAbsolutePath());
        return dataPath;
    }
    
    private static void saveResults(String outputBaseDir, String cityName, Map<String, Object> results) throws IOException {
        @SuppressWarnings("unchecked")
        List<ClusterLeaderSolver.CustomerAssignment> assignments =
            (List<ClusterLeaderSolver.CustomerAssignment>) results.get("assignments");
        
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");
        String timestamp = LocalDateTime.now().format(formatter);
        
        Path cityDir = Paths.get(outputBaseDir, cityName);
        Files.createDirectories(cityDir);
        
        Path assignmentsPath = cityDir.resolve("assignments_" + timestamp + ".csv");
        try (java.io.BufferedWriter writer = Files.newBufferedWriter(assignmentsPath, StandardCharsets.UTF_8)) {
            writer.write("customer_id,cluster_id,medoid_id,medoid_index,distance_km");
            writer.newLine();
            for (ClusterLeaderSolver.CustomerAssignment assignment : assignments) {
                writer.write(String.join(",",
                    assignment.customerId,
                    Integer.toString(assignment.clusterId),
                    assignment.medoidId,
                    Integer.toString(assignment.medoidIndex),
                    String.format(Locale.US, "%.6f", assignment.distanceKm)
                ));
                writer.newLine();
            }
        }
        
        Path summaryPath = cityDir.resolve("summary_" + timestamp + ".txt");
        try (java.io.BufferedWriter writer = Files.newBufferedWriter(summaryPath, StandardCharsets.UTF_8)) {
            writer.write("City: " + cityName);
            writer.newLine();
            writer.write("Best P: " + results.get("bestP"));
            writer.newLine();
            writer.write("Best Objective: " + results.get("bestObjective"));
            writer.newLine();
            writer.write("Total assignments: " + assignments.size());
            writer.newLine();
            writer.write("Assignments file: " + assignmentsPath.toAbsolutePath());
            writer.newLine();
        }
        
        System.out.println("💾 Đã lưu kết quả vào: " + cityDir.toAbsolutePath());
    }
}


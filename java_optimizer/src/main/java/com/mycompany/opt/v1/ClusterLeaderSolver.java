package com.mycompany.opt.v1;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.Comparator;

/**
 * Cluster Leader Solver - Java implementation of the Python clustering algorithm
 * Version 1: Original implementation
 * 
 * Features:
 * - K-means clustering (MiniBatchKMeans for large N, PAM k-medoids for small N)
 * - TSP solving (Nearest Neighbor + 2-opt)
 * - Haversine distance calculation
 * - Graph-based shortest path (optional)
 * - Objective: intra-cluster distance + alpha * route cost
 * 
 * Based on: backend/optimizer/test.py
 */
public class ClusterLeaderSolver {
    private static final double EARTH_RADIUS_KM = 6371.0;
    private static final int MAX_2OPT_ITERATIONS = 5000;
    private static final int PAM_MAX_ITERATIONS = 200;
    private static final int DEFAULT_MAX_EXACT_N = 2000;
    
    private List<Customer> customers;
    private Map<String, Map<String, Road>> roadMap;
    private Random random;
    private int numThreads;
    private ExecutorService executorService;
    
    // Results
    private static class ClusterResult {
        int P;
        double objective;
        double intraClusterCost;
        double routeCost;
        List<Integer> medoidIndices;
        
        ClusterResult(int P, double objective, double intraClusterCost, double routeCost, List<Integer> medoidIndices) {
            this.P = P;
            this.objective = objective;
            this.intraClusterCost = intraClusterCost;
            this.routeCost = routeCost;
            this.medoidIndices = medoidIndices;
        }
    }
    
    public ClusterLeaderSolver(List<Customer> customers, Map<String, Map<String, Road>> roadMap) {
        this.customers = customers;
        this.roadMap = roadMap;
        this.random = new Random(0); // Fixed seed for reproducibility
        this.numThreads = Runtime.getRuntime().availableProcessors();
        this.executorService = Executors.newFixedThreadPool(numThreads);
    }
    
    /**
     * Main pipeline: cluster customers and optimize routes
     */
    public Map<String, Object> runPipeline(int P_min, int P_max, double alpha, 
                                          double depotLat, double depotLon,
                                          boolean useGraph, int maxExactN) {
        long startTime = System.currentTimeMillis();
        
        int N = customers.size();
        System.out.println("📊 Loaded " + N + " customers.");
        
        // Sanitize P
        P_min = Math.max(1, P_min);
        P_max = Math.min(P_max, N - 1);
        if (P_min > P_max) {
            throw new IllegalArgumentException("Invalid P_min/P_max");
        }
        
        // Prepare coordinates
        List<double[]> coords = customers.stream()
            .map(c -> new double[]{c.getLatitude(), c.getLongitude()})
            .collect(Collectors.toList());
        
        // Decide strategy
        boolean useExact = (N <= maxExactN);
        System.out.println("Using exact PAM? " + useExact + " (N=" + N + ", maxExactN=" + maxExactN + ")");
        
        List<ClusterResult> results = new ArrayList<>();
        ClusterResult best = null;
        
        // Try different P values
        for (int P = P_min; P <= P_max; P++) {
            System.out.println("Trying P=" + P + " ...");
            
            ClusterResult result;
            if (useExact) {
                result = solveWithExactPAM(P, coords, alpha, depotLat, depotLon);
            } else {
                result = solveWithKMeans(P, coords, alpha, depotLat, depotLon);
            }
            
            results.add(result);
            System.out.println(String.format("  P=%d: obj=%.3f, intra=%.3f, route=%.3f", 
                P, result.objective, result.intraClusterCost, result.routeCost));
            
            if (best == null || result.objective < best.objective) {
                best = result;
            }
        }
        
        if (best == null) {
            throw new RuntimeException("No result obtained");
        }
        
        System.out.println("Best P=" + best.P + ", objective=" + String.format("%.3f", best.objective));
        
        // Build final assignments
        Map<String, Object> output = buildFinalAssignments(best, coords);
        
        long elapsed = System.currentTimeMillis() - startTime;
        System.out.println("Done. Elapsed " + (elapsed / 1000.0) + "s");
        
        output.put("best", best);
        output.put("results", results);
        output.put("elapsedSeconds", elapsed / 1000.0);
        
        return output;
    }
    
    /**
     * Solve using exact PAM k-medoids (for small N)
     */
    private ClusterResult solveWithExactPAM(int P, List<double[]> coords, double alpha, 
                                           double depotLat, double depotLon) {
        int N = coords.size();
        
        // Build distance matrix
        double[][] D = buildDistanceMatrix(coords);
        
        // Run PAM
        PAMResult pamResult = pamMedoids(D, P);
        List<Integer> medoidIndices = pamResult.medoids;
        
        // Calculate intra-cluster cost: sum of min distance from each point to all medoids
        // This matches Python: np.sum(np.min(D[:, medoid_indices], axis=1))
        double intraCost = 0.0;
        for (int i = 0; i < N; i++) {
            double minDist = D[i][medoidIndices.get(0)];
            for (int m = 1; m < medoidIndices.size(); m++) {
                double dist = D[i][medoidIndices.get(m)];
                if (dist < minDist) {
                    minDist = dist;
                }
            }
            intraCost += minDist;
        }
        
        // Calculate route cost (TSP on depot + medoids)
        double routeCost = calculateRouteCost(medoidIndices, coords, depotLat, depotLon);
        
        double objective = intraCost + alpha * routeCost;
        
        return new ClusterResult(P, objective, intraCost, routeCost, medoidIndices);
    }
    
    /**
     * Solve using K-means (scalable for large N)
     */
    private ClusterResult solveWithKMeans(int P, List<double[]> coords, double alpha,
                                         double depotLat, double depotLon) {
        int N = coords.size();
        
        // K-means clustering
        KMeansResult kmeansResult = kMeansClustering(coords, P);
        List<Integer> labels = kmeansResult.labels;
        List<double[]> centroids = kmeansResult.centroids;
        
        // Choose medoid per cluster (point nearest to centroid)
        List<Integer> medoidIndices = new ArrayList<>();
        for (int k = 0; k < P; k++) {
            final int clusterId = k;
            List<Integer> clusterPoints = IntStream.range(0, N)
                .filter(i -> labels.get(i) == clusterId)
                .boxed()
                .collect(Collectors.toList());
            
            if (clusterPoints.isEmpty()) {
                medoidIndices.add(random.nextInt(N));
                continue;
            }
            
            // Find point closest to centroid
            double[] centroid = centroids.get(k);
            int bestMedoid = clusterPoints.stream()
                .min(Comparator.comparingDouble(i -> 
                    haversineDistance(coords.get(i), centroid)))
                .orElse(clusterPoints.get(0));
            
            medoidIndices.add(bestMedoid);
        }
        
        // Calculate intra-cluster cost: recalculate assignments from medoids
        // This matches Python: assign = np.argmin(all_to_meds, axis=1)
        // Then: intra = float(all_to_meds[np.arange(N), assign].sum())
        double intraCost = 0.0;
        for (int i = 0; i < N; i++) {
            double[] customerCoord = coords.get(i);
            double minDist = haversineDistance(customerCoord, coords.get(medoidIndices.get(0)));
            for (int m = 1; m < medoidIndices.size(); m++) {
                double dist = haversineDistance(customerCoord, coords.get(medoidIndices.get(m)));
                if (dist < minDist) {
                    minDist = dist;
                }
            }
            intraCost += minDist;
        }
        
        // Calculate route cost
        double routeCost = calculateRouteCost(medoidIndices, coords, depotLat, depotLon);
        
        double objective = intraCost + alpha * routeCost;
        
        return new ClusterResult(P, objective, intraCost, routeCost, medoidIndices);
    }
    
    /**
     * PAM (Partitioning Around Medoids) k-medoids algorithm
     */
    private PAMResult pamMedoids(double[][] distanceMatrix, int k) {
        int N = distanceMatrix.length;
        
        // Initialize medoids randomly
        List<Integer> medoids = new ArrayList<>();
        Set<Integer> used = new HashSet<>();
        while (medoids.size() < k) {
            int candidate = random.nextInt(N);
            if (!used.contains(candidate)) {
                medoids.add(candidate);
                used.add(candidate);
            }
        }
        
        // PAM iterations
        for (int iter = 0; iter < PAM_MAX_ITERATIONS; iter++) {
            // Assign points to nearest medoid
            int[] assignments = new int[N];
            for (int i = 0; i < N; i++) {
                int bestMedoid = 0;
                double bestDist = distanceMatrix[i][medoids.get(0)];
                for (int m = 1; m < medoids.size(); m++) {
                    double dist = distanceMatrix[i][medoids.get(m)];
                    if (dist < bestDist) {
                        bestDist = dist;
                        bestMedoid = m;
                    }
                }
                assignments[i] = bestMedoid;
            }
            
            // Calculate current cost
            double currentCost = 0.0;
            for (int i = 0; i < N; i++) {
                currentCost += distanceMatrix[i][medoids.get(assignments[i])];
            }
            
            // Try swapping medoids
            boolean improved = false;
            for (int mIdx = 0; mIdx < medoids.size(); mIdx++) {
                for (int candidate = 0; candidate < N; candidate++) {
                    if (medoids.contains(candidate)) {
                        continue;
                    }
                    
                    // Try swap
                    List<Integer> newMedoids = new ArrayList<>(medoids);
                    newMedoids.set(mIdx, candidate);
                    
                    // Reassign
                    int[] newAssignments = new int[N];
                    double newCost = 0.0;
                    for (int i = 0; i < N; i++) {
                        int bestMedoid = 0;
                        double bestDist = distanceMatrix[i][newMedoids.get(0)];
                        for (int m = 1; m < newMedoids.size(); m++) {
                            double dist = distanceMatrix[i][newMedoids.get(m)];
                            if (dist < bestDist) {
                                bestDist = dist;
                                bestMedoid = m;
                            }
                        }
                        newAssignments[i] = bestMedoid;
                        newCost += bestDist;
                    }
                    
                    if (newCost + 1e-9 < currentCost) {
                        medoids = newMedoids;
                        improved = true;
                        break;
                    }
                }
                if (improved) {
                    break;
                }
            }
            
            if (!improved) {
                break;
            }
        }
        
        // Final assignment
        int[] finalAssignments = new int[N];
        for (int i = 0; i < N; i++) {
            int bestMedoid = 0;
            double bestDist = distanceMatrix[i][medoids.get(0)];
            for (int m = 1; m < medoids.size(); m++) {
                double dist = distanceMatrix[i][medoids.get(m)];
                if (dist < bestDist) {
                    bestDist = dist;
                    bestMedoid = m;
                }
            }
            finalAssignments[i] = bestMedoid;
        }
        
        return new PAMResult(medoids, finalAssignments);
    }
    
    private static class PAMResult {
        List<Integer> medoids;
        int[] assignments;
        
        PAMResult(List<Integer> medoids, int[] assignments) {
            this.medoids = medoids;
            this.assignments = assignments;
        }
    }
    
    /**
     * K-means clustering (simplified version)
     */
    private KMeansResult kMeansClustering(List<double[]> coords, int k) {
        int N = coords.size();
        int maxIterations = 200;
        
        // Initialize centroids randomly
        List<double[]> centroids = new ArrayList<>();
        Set<Integer> used = new HashSet<>();
        while (centroids.size() < k) {
            int idx = random.nextInt(N);
            if (!used.contains(idx)) {
                centroids.add(new double[]{coords.get(idx)[0], coords.get(idx)[1]});
                used.add(idx);
            }
        }
        
        List<Integer> labels = new ArrayList<>(Collections.nCopies(N, 0));
        
        for (int iter = 0; iter < maxIterations; iter++) {
            // Assign points to nearest centroid
            boolean changed = false;
            for (int i = 0; i < N; i++) {
                double[] point = coords.get(i);
                int bestCluster = 0;
                double bestDist = haversineDistance(point, centroids.get(0));
                for (int c = 1; c < k; c++) {
                    double dist = haversineDistance(point, centroids.get(c));
                    if (dist < bestDist) {
                        bestDist = dist;
                        bestCluster = c;
                    }
                }
                if (labels.get(i) != bestCluster) {
                    labels.set(i, bestCluster);
                    changed = true;
                }
            }
            
            if (!changed) {
                break;
            }
            
            // Update centroids
            for (int c = 0; c < k; c++) {
                final int clusterId = c;
                List<double[]> clusterPoints = IntStream.range(0, N)
                    .filter(i -> labels.get(i) == clusterId)
                    .mapToObj(coords::get)
                    .collect(Collectors.toList());
                
                if (!clusterPoints.isEmpty()) {
                    double sumLat = clusterPoints.stream().mapToDouble(p -> p[0]).sum();
                    double sumLon = clusterPoints.stream().mapToDouble(p -> p[1]).sum();
                    centroids.set(c, new double[]{
                        sumLat / clusterPoints.size(),
                        sumLon / clusterPoints.size()
                    });
                }
            }
        }
        
        return new KMeansResult(centroids, labels);
    }
    
    private static class KMeansResult {
        List<double[]> centroids;
        List<Integer> labels;
        
        KMeansResult(List<double[]> centroids, List<Integer> labels) {
            this.centroids = centroids;
            this.labels = labels;
        }
    }
    
    /**
     * Calculate TSP route cost using Nearest Neighbor + 2-opt
     */
    private double calculateRouteCost(List<Integer> medoidIndices, List<double[]> coords,
                                      double depotLat, double depotLon) {
        if (medoidIndices.isEmpty()) {
            return 0.0;
        }
        
        // Build distance matrix for depot + medoids
        List<double[]> nodes = new ArrayList<>();
        nodes.add(new double[]{depotLat, depotLon});
        for (int idx : medoidIndices) {
            nodes.add(coords.get(idx));
        }
        
        int M = nodes.size();
        double[][] D = new double[M][M];
        for (int i = 0; i < M; i++) {
            for (int j = 0; j < M; j++) {
                D[i][j] = haversineDistance(nodes.get(i), nodes.get(j));
            }
        }
        
        // Nearest Neighbor
        List<Integer> tour = new ArrayList<>();
        tour.add(0); // Start at depot
        Set<Integer> unvisited = new HashSet<>();
        for (int i = 1; i < M; i++) {
            unvisited.add(i);
        }
        
        int current = 0;
        while (!unvisited.isEmpty()) {
            final int currentFinal = current; // Make final for lambda
            int next = unvisited.stream()
                .min(Comparator.comparingDouble(i -> D[currentFinal][i]))
                .orElse(unvisited.iterator().next());
            tour.add(next);
            unvisited.remove(next);
            current = next;
        }
        tour.add(0); // Return to depot
        
        // 2-opt improvement
        // Note: tour has size M+1 (includes return to depot at the end)
        boolean improved = true;
        int iterations = 0;
        int tourSize = tour.size(); // M+1 (includes final depot return)
        
        while (improved && iterations < MAX_2OPT_ITERATIONS) {
            improved = false;
            iterations++;
            
            // Only optimize the middle part (exclude first and last depot)
            for (int i = 1; i < tourSize - 2; i++) {
                for (int j = i + 1; j < tourSize - 1; j++) {
                    if (j - i == 1) {
                        continue;
                    }
                    
                    int a = tour.get(i - 1);
                    int b = tour.get(i);
                    int c = tour.get(j);
                    int d = tour.get(j + 1);
                    
                    // Check if reversing segment [i, j] improves the tour
                    if (D[a][c] + D[b][d] < D[a][b] + D[c][d] - 1e-9) {
                        // Reverse segment from i to j (inclusive)
                        Collections.reverse(tour.subList(i, j + 1));
                        improved = true;
                    }
                }
            }
        }
        
        // Calculate total length
        double length = 0.0;
        for (int i = 0; i < tour.size() - 1; i++) {
            int from = tour.get(i);
            int to = tour.get(i + 1);
            // Safety check
            if (from >= 0 && from < M && to >= 0 && to < M) {
                length += D[from][to];
            }
        }
        
        return length;
    }
    
    /**
     * Build distance matrix using haversine (and graph if available)
     */
    private double[][] buildDistanceMatrix(List<double[]> coords) {
        int N = coords.size();
        double[][] D = new double[N][N];
        
        // TODO: Use graph shortest paths if available
        // For now, use haversine for all pairs
        
        for (int i = 0; i < N; i++) {
            D[i][i] = 0.0;
            for (int j = i + 1; j < N; j++) {
                double dist = haversineDistance(coords.get(i), coords.get(j));
                D[i][j] = dist;
                D[j][i] = dist;
            }
        }
        
        return D;
    }
    
    /**
     * Haversine distance between two points (lat, lon)
     */
    private double haversineDistance(double[] a, double[] b) {
        double lat1 = Math.toRadians(a[0]);
        double lon1 = Math.toRadians(a[1]);
        double lat2 = Math.toRadians(b[0]);
        double lon2 = Math.toRadians(b[1]);
        
        double dLat = lat2 - lat1;
        double dLon = lon2 - lon1;
        
        double sinDLat = Math.sin(dLat / 2.0);
        double sinDLon = Math.sin(dLon / 2.0);
        double a_val = sinDLat * sinDLat + 
                      Math.cos(lat1) * Math.cos(lat2) * sinDLon * sinDLon;
        a_val = Math.min(1.0, a_val);
        
        return 2 * EARTH_RADIUS_KM * Math.asin(Math.sqrt(a_val));
    }
    
    /**
     * Build final customer assignments
     */
    private Map<String, Object> buildFinalAssignments(ClusterResult best, List<double[]> coords) {
        int N = customers.size();
        List<Integer> medoidIndices = best.medoidIndices;
        
        // Calculate distances to medoids
        List<double[]> medoidCoords = medoidIndices.stream()
            .map(coords::get)
            .collect(Collectors.toList());
        
        List<CustomerAssignment> assignments = new ArrayList<>();
        for (int i = 0; i < N; i++) {
            double[] customerCoord = coords.get(i);
            
            // Find nearest medoid
            int bestCluster = 0;
            double bestDist = haversineDistance(customerCoord, medoidCoords.get(0));
            for (int c = 1; c < medoidCoords.size(); c++) {
                double dist = haversineDistance(customerCoord, medoidCoords.get(c));
                if (dist < bestDist) {
                    bestDist = dist;
                    bestCluster = c;
                }
            }
            
            int medoidGlobalIdx = medoidIndices.get(bestCluster);
            assignments.add(new CustomerAssignment(
                customers.get(i).getCustomerId(),
                bestCluster,
                medoidGlobalIdx,
                customers.get(medoidGlobalIdx).getCustomerId(),
                bestDist
            ));
        }
        
        Map<String, Object> output = new HashMap<>();
        output.put("assignments", assignments);
        output.put("bestP", best.P);
        output.put("bestObjective", best.objective);
        
        return output;
    }
    
    public static class CustomerAssignment {
        String customerId;
        int clusterId;
        int medoidIndex;
        String medoidId;
        double distanceKm;
        
        CustomerAssignment(String customerId, int clusterId, int medoidIndex, 
                          String medoidId, double distanceKm) {
            this.customerId = customerId;
            this.clusterId = clusterId;
            this.medoidIndex = medoidIndex;
            this.medoidId = medoidId;
            this.distanceKm = distanceKm;
        }
        
        @Override
        public String toString() {
            return String.format("Customer %s -> Cluster %d (Medoid: %s, Dist: %.2f km)",
                customerId, clusterId, medoidId, distanceKm);
        }
    }
    
    public void shutdown() {
        if (executorService != null && !executorService.isShutdown()) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
}


package io.grpc.analytics;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.api.java.function.Function;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class KMeansClusteringAnalytics implements Serializable {

    private static final long serialVersionUID = 1L;
    
    private final transient JavaSparkContext jsc;
    private final String datasetPath;
    private final String outputDir;
    private final String datasetName;
    private final int numClusters;
    private final int numIterations;

    /**
     * Constructor for the KMeansClusteringAnalytics class.
     * 
     * @param jsc           The JavaSparkContext to use for the analysis
     * @param datasetPath   The path to the input dataset
     * @param outputDir     The directory to save the output results
     * @param datasetName   The name of the dataset for output file naming
     * @param numClusters   The number of clusters to create
     * @param numIterations The number of iterations for KMeans
     */
    public KMeansClusteringAnalytics(
            JavaSparkContext jsc,
            String datasetPath,
            String outputDir,
            String datasetName,
            int numClusters,
            int numIterations) {
        this.jsc = jsc;
        this.datasetPath = datasetPath;
        this.outputDir = outputDir;
        this.datasetName = datasetName;
        this.numClusters = numClusters;
        this.numIterations = numIterations;
    }

    /**
     * Parse a CSV line respecting quoted values that may contain commas.
     * 
     * @param line The CSV line to parse
     * @return Array of parsed values
     */
    private static String[] parseCSVLine(String line) {
        List<String> result = new ArrayList<>();
        boolean inQuotes = false;
        boolean inDoubleQuotes = false;
        StringBuilder currentField = new StringBuilder();
        
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            
            if (c == '"' && !inQuotes) {
                inDoubleQuotes = !inDoubleQuotes;
            } else if (c == '\'' && !inDoubleQuotes) {
                inQuotes = !inQuotes;
            } else if (c == ',' && !inQuotes && !inDoubleQuotes) {
                result.add(currentField.toString().trim());
                currentField = new StringBuilder();
            } else {
                currentField.append(c);
            }
        }
        
        // Add the last field
        result.add(currentField.toString().trim());
        
        return result.toArray(new String[0]);
    }

    /**
     * Run K-means clustering on the provided dataset.
     * 
     * @return A summary of the clustering results
     * @throws IOException If an error occurs during analysis
     */
    public ClusteringResult runClustering() throws IOException {
        JavaRDD<String> data = jsc.textFile(datasetPath);

        // First pass: identify categorical columns and their possible values
        // Assume first line is header
        String header = data.first();
        String[] columns = parseCSVLine(header);
        int numColumns = columns.length;

        // Skip header for data processing
        JavaRDD<String> dataWithoutHeader = data.filter(line -> !line.equals(header));

        // Identify which columns are categorical and collect their unique values
        boolean[] isCategorical = new boolean[numColumns];
        Map<Integer, Set<String>> categoricalValues = new HashMap<>();
        Map<Integer, Map<String, Integer>> categoricalMappings = new HashMap<>();

        // First scan: determine which columns are categorical
        List<String[]> rows = dataWithoutHeader.map(new Function<String, String[]>() {
            @Override
            public String[] call(String line) {
                return parseCSVLine(line);
            }
        }).collect();
        for (int i = 0; i < numColumns; i++) {
            boolean categorical = false;
            Set<String> uniqueValues = new HashSet<>();

            for (String[] row : rows) {
                String value = row[i].trim();
                // Remove surrounding quotes if present
                if ((value.startsWith("\"") && value.endsWith("\"")) || 
                    (value.startsWith("'") && value.endsWith("'"))) {
                    value = value.substring(1, value.length() - 1);
                }
                uniqueValues.add(value);
                try {
                    Double.parseDouble(value);
                } catch (NumberFormatException e) {
                    categorical = true;
                }
            }

            isCategorical[i] = categorical;
            if (categorical) {
                categoricalValues.put(i, uniqueValues);

                // Create mapping for one-hot encoding
                Map<String, Integer> valueMap = new HashMap<>();
                int index = 0;
                for (String value : uniqueValues) {
                    valueMap.put(value, index++);
                }
                categoricalMappings.put(i, valueMap);
            }
        }

        // Calculate the final vector size after one-hot encoding
        int vectorSize = 0;
        for (int i = 0; i < numColumns; i++) {
            if (isCategorical[i]) {
                vectorSize += categoricalValues.get(i).size();
            } else {
                vectorSize++;
            }
        }

        // Second pass: compute min and max for numerical columns for normalization
        double[] minValues = new double[numColumns];
        double[] maxValues = new double[numColumns];
        Arrays.fill(minValues, Double.MAX_VALUE);
        Arrays.fill(maxValues, Double.MIN_VALUE);

        for (String[] row : rows) {
            for (int i = 0; i < numColumns; i++) {
                if (!isCategorical[i]) {
                    String value = row[i].trim();
                    // Remove surrounding quotes if present
                    if ((value.startsWith("\"") && value.endsWith("\"")) || 
                        (value.startsWith("'") && value.endsWith("'"))) {
                        value = value.substring(1, value.length() - 1);
                    }
                    double numValue = Double.parseDouble(value);
                    minValues[i] = Math.min(minValues[i], numValue);
                    maxValues[i] = Math.max(maxValues[i], numValue);
                }
            }
        }



        // Convert data to feature vectors with one-hot encoding for categorical
        // variables
        // Store the original data and transformation information for later use
        final boolean[] finalIsCategorical = isCategorical;
        final double[] finalMinValues = minValues;
        final double[] finalMaxValues = maxValues;
        final Map<Integer, Map<String, Integer>> finalCategoricalMappings = categoricalMappings;
        final Map<Integer, Set<String>> finalCategoricalValues = categoricalValues;

        // Convert to JavaRDD for processing
        JavaRDD<String[]> rowsRDD = jsc.parallelize(rows);

        // Create a mapping of raw data to feature vectors
        final int finalVectorSize = vectorSize;
        JavaRDD<Vector> parsedData = rowsRDD.map(new Function<String[], Vector>() {
            @Override
            public Vector call(String[] values) {
                double[] features = new double[finalVectorSize];

                int featureIndex = 0;
                for (int i = 0; i < values.length; i++) {
                    String value = values[i].trim();
                    // Remove surrounding quotes if present
                    if ((value.startsWith("\"") && value.endsWith("\"")) || 
                        (value.startsWith("'") && value.endsWith("'"))) {
                        value = value.substring(1, value.length() - 1);
                    }

                    if (finalIsCategorical[i]) {
                        // One-hot encoding
                        Map<String, Integer> valueMap = finalCategoricalMappings.get(i);
                        int oneHotIndex = valueMap.get(value);
                        for (int j = 0; j < finalCategoricalValues.get(i).size(); j++) {
                            features[featureIndex++] = (j == oneHotIndex) ? 1.0 : 0.0;
                        }
                    } else {
                        // Normalize numerical values
                        if (finalMaxValues[i] > finalMinValues[i]) {
                            double normalizedValue = (Double.parseDouble(value) - finalMinValues[i]) /
                                    (finalMaxValues[i] - finalMinValues[i]);
                            features[featureIndex++] = normalizedValue;
                        } else {
                            // Handle the case where min and max are the same (constant feature)
                            features[featureIndex++] = 0.0;
                        }
                    }
                }

                return Vectors.dense(features);
            }
        });

        parsedData.cache();

        // Cluster the data using KMeans
        KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);

        // Predict clusters for all data points
        List<Vector> allPoints = parsedData.collect();
        List<Integer> allPredictions = new ArrayList<>();

        for (Vector point : allPoints) {
            allPredictions.add(clusters.predict(point));
        }



        // Write results to CSV file with complete data for analysis
        String outputFilePath = outputDir + "/"+System.getProperty("user.name")+"_kmeans_" + datasetName +
        ".csv";
        File outputFile = new File(outputFilePath);
        File outputDirectory = outputFile.getParentFile();

        // Check if the directory exists
        if (outputDirectory != null && !outputDirectory.exists()) {
            // Create the directory and any necessary parent directories
            if (outputDirectory.mkdirs()) {
                System.out.println("Directory created successfully: " + outputDirectory.getAbsolutePath());
            }
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath))) {
            // Write header with additional columns for cluster
            writer.write(header + ",cluster\n");

            // Write data rows with cluster assignments
            for (int i = 0; i < rows.size(); i++) {
                StringBuilder sb = new StringBuilder();

                // Write original data values
                for (int j = 0; j < rows.get(i).length; j++) {
                    sb.append(rows.get(i)[j]);
                    sb.append(",");
                }

                // Add cluster assignment
                sb.append(allPredictions.get(i));
                sb.append("\n");

                writer.write(sb.toString());
            }
        }

        // Output to console
        System.out.println("\nCluster centers:");
        for (Vector center : clusters.clusterCenters()) {
            System.out.println(" " + center);
        }

        double cost = clusters.computeCost(parsedData.rdd());
        System.out.println("Within Set Sum of Squared Errors = " + cost);

        return new ClusteringResult(
                clusters,
                cost,
                outputFilePath,
                allPredictions);
    }

    /**
     * Class to hold the results of the clustering analysis.
     */
    public static class ClusteringResult implements Serializable {
        private static final long serialVersionUID = 1L;
        
        private final transient KMeansModel model;
        private final double cost;
        private final String outputFilePath;
        private final List<Integer> clusterAssignments;

        public ClusteringResult(
                KMeansModel model,
                double cost,
                String outputFilePath,
                List<Integer> clusterAssignments) {
            this.model = model;
            this.cost = cost;
            this.outputFilePath = outputFilePath;
            this.clusterAssignments = clusterAssignments;
        }

        public KMeansModel getModel() {
            return model;
        }

        public double getCost() {
            return cost;
        }

        public String getOutputFilePath() {
            return outputFilePath;
        }

        public List<Integer> getClusterAssignments() {
            return clusterAssignments;
        }

        @Override
        public String toString() {
            return "ClusteringResult{" +
                    "cost=" + cost +
                    ", outputFilePath='" + outputFilePath + '\'' +
                    ", numClusters=" + (model != null ? model.clusterCenters().length : 0) +
                    ", numDataPoints=" + clusterAssignments.size() +
                    '}';
        }
    }
}
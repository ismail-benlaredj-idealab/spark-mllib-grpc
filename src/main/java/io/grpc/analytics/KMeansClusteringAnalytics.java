package io.grpc.analytics;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class KMeansClusteringAnalytics {

    private final JavaSparkContext jsc;
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
        String[] columns = header.split(",");
        int numColumns = columns.length;

        // Skip header for data processing
        JavaRDD<String> dataWithoutHeader = data.filter(line -> !line.equals(header));

        // Identify which columns are categorical and collect their unique values
        boolean[] isCategorical = new boolean[numColumns];
        Map<Integer, Set<String>> categoricalValues = new HashMap<>();
        Map<Integer, Map<String, Integer>> categoricalMappings = new HashMap<>();

        // First scan: determine which columns are categorical
        List<String[]> rows = dataWithoutHeader.map(line -> line.split(",")).collect();
        for (int i = 0; i < numColumns; i++) {
            boolean categorical = false;
            Set<String> uniqueValues = new HashSet<>();

            for (String[] row : rows) {
                String value = row[i].trim();
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
                    double value = Double.parseDouble(row[i].trim());
                    minValues[i] = Math.min(minValues[i], value);
                    maxValues[i] = Math.max(maxValues[i], value);
                }
            }
        }

        // Find two numerical features for direct plotting
        List<Integer> numericalColumns = new ArrayList<>();
        for (int i = 0; i < numColumns; i++) {
            if (!isCategorical[i]) {
                numericalColumns.add(i);
            }
        }

        // For direct plotting, use the first two numerical features if available
        int xAxisColumn = -1;
        int yAxisColumn = -1;

        if (numericalColumns.size() >= 2) {
            xAxisColumn = numericalColumns.get(0);
            yAxisColumn = numericalColumns.get(1);
            System.out.println("Selected columns for direct plotting: " +
                    columns[xAxisColumn] + " and " + columns[yAxisColumn]);
        } else {
            System.out.println("Not enough numerical columns for direct plotting. Will use dimensionality reduction.");
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
        JavaRDD<Vector> parsedData = rowsRDD.map(values -> {
            double[] features = new double[finalVectorSize];

            int featureIndex = 0;
            for (int i = 0; i < values.length; i++) {
                String value = values[i].trim();

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

        // Prepare 2D coordinates for plotting
        double[][] plotCoordinates = new double[rows.size()][2];

        if (xAxisColumn >= 0 && yAxisColumn >= 0) {
            // Use selected numerical features directly
            for (int i = 0; i < rows.size(); i++) {
                double xValue = Double.parseDouble(rows.get(i)[xAxisColumn].trim());
                double yValue = Double.parseDouble(rows.get(i)[yAxisColumn].trim());

                plotCoordinates[i][0] = xValue;
                plotCoordinates[i][1] = yValue;
            }
        } else {
            // Simplified approach: find two dimensions with the highest variance
            double[] variances = new double[vectorSize];
            double[] means = new double[vectorSize];

            // Calculate means
            for (Vector point : allPoints) {
                for (int i = 0; i < vectorSize; i++) {
                    means[i] += point.apply(i);
                }
            }
            for (int i = 0; i < vectorSize; i++) {
                means[i] /= allPoints.size();
            }

            // Calculate variances
            for (Vector point : allPoints) {
                for (int i = 0; i < vectorSize; i++) {
                    variances[i] += Math.pow(point.apply(i) - means[i], 2);
                }
            }
            for (int i = 0; i < vectorSize; i++) {
                variances[i] /= allPoints.size();
            }

            // Find two dimensions with highest variance
            int dim1 = 0;
            int dim2 = 1;
            for (int i = 2; i < vectorSize; i++) {
                if (variances[i] > variances[dim1]) {
                    dim2 = dim1;
                    dim1 = i;
                } else if (variances[i] > variances[dim2]) {
                    dim2 = i;
                }
            }

            // Project to these two dimensions
            for (int i = 0; i < allPoints.size(); i++) {
                plotCoordinates[i][0] = allPoints.get(i).apply(dim1);
                plotCoordinates[i][1] = allPoints.get(i).apply(dim2);
            }
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
            // Write header with additional columns for plotting coordinates
            if (xAxisColumn >= 0 && yAxisColumn >= 0) {
                writer.write(header + ",plot_x,plot_y,cluster\n");
            } else {
                writer.write(header + ",dim1,dim2,cluster\n");
            }

            // Write data rows with cluster assignments
            for (int i = 0; i < rows.size(); i++) {
                StringBuilder sb = new StringBuilder();

                // Write original data values
                for (int j = 0; j < rows.get(i).length; j++) {
                    sb.append(rows.get(i)[j]);
                    sb.append(",");
                }

                // Add plotting coordinates
                sb.append(plotCoordinates[i][0]).append(",");
                sb.append(plotCoordinates[i][1]).append(",");

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
                allPredictions,
                plotCoordinates);
    }

    /**
     * Class to hold the results of the clustering analysis.
     */
    public static class ClusteringResult {
        private final KMeansModel model;
        private final double cost;
        private final String outputFilePath;
        private final List<Integer> clusterAssignments;
        private final double[][] plotCoordinates;

        public ClusteringResult(
                KMeansModel model,
                double cost,
                String outputFilePath,
                List<Integer> clusterAssignments,
                double[][] plotCoordinates) {
            this.model = model;
            this.cost = cost;
            this.outputFilePath = outputFilePath;
            this.clusterAssignments = clusterAssignments;
            this.plotCoordinates = plotCoordinates;
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

        public double[][] getPlotCoordinates() {
            return plotCoordinates;
        }

        @Override
        public String toString() {
            return "ClusteringResult{" +
                    "cost=" + cost +
                    ", outputFilePath='" + outputFilePath + '\'' +
                    ", numClusters=" + model.clusterCenters().length +
                    ", numDataPoints=" + clusterAssignments.size() +
                    '}';
        }
    }
}
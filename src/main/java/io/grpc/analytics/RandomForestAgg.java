package io.grpc.analytics;




import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.regression.RandomForestRegressionModel;
import org.apache.spark.ml.tree.TreeEnsembleModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class RandomForestAgg {

    private SparkSession spark;
    private String[] nodesFolder;
    private String outputFolder;
    private String labelColumn;
    private boolean isClassification; // Flag to determine if this is classification or regression

    public RandomForestAgg(String[] nodesFolder, String outputFolder, String labelColumn, boolean isClassification) {
        this.nodesFolder = nodesFolder;
        this.outputFolder = outputFolder;
        this.labelColumn = labelColumn;
        this.isClassification = isClassification;
        
        // Initialize Spark session
        this.spark = SparkSession.builder()
                .appName("RandomForestAgg")
                .master("local[*]")
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
                .getOrCreate();
    }

    public static void main(String[] args) {

        String outputFolder ="output/combined";
        String labelColumn = "claim";
        boolean isClassification = false;
        
        // Collect all node folders
        String[] nodesFolder = getAllFolders("/path/to/your/directory");



        RandomForestAgg combiner = new RandomForestAgg(nodesFolder, outputFolder, labelColumn, isClassification);
        combiner.combineAllResults();
    }

    /**
     * Main method to combine all results from nodes
     */
    public void combineAllResults() {
        try {
            // Create output directory
            Files.createDirectories(Paths.get(outputFolder));
            
            // Combine prediction results
            combinePredictions();
            
            // Combine performance metrics
            combinePerformanceMetrics();
            
            // Combine feature importances
            combineFeatureImportances();
            
            // Aggregate model information
            aggregateModelInformation();
            
            // Combine decision trees
            combineDecisionTrees();
            
            System.out.println("Successfully combined results from all nodes");
        } catch (Exception e) {
            System.err.println("Error combining node results: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (spark != null) {
                spark.stop();
            }
        }
    }

    /**
     * Combines predictions from all nodes and creates ensemble predictions
     */
    private void combinePredictions() throws IOException {
        List<Dataset<Row>> allPredictions = new ArrayList<Dataset<Row>>();
        
        // Load predictions from each node
        for (int i = 0; i < nodesFolder.length; i++) {
            String nodeFolder = nodesFolder[i];
            String predictionsPath = nodeFolder + "/predictions";
            System.out.println("Loading predictions from node folder----------------------: " + predictionsPath);
            if (Files.exists(Paths.get(predictionsPath))) {
                Dataset<Row> nodePredictions = spark.read()
                        .option("header", "true")
                        .option("inferSchema", "true")
                        .csv(predictionsPath);
                
                // Add node identifier
                nodePredictions = nodePredictions.withColumn("source_node", 
                        org.apache.spark.sql.functions.lit(Paths.get(nodeFolder).getFileName().toString()));
                
                allPredictions.add(nodePredictions);
            }
        }
        
        if (allPredictions.isEmpty()) {
            System.out.println("No prediction data found in node folders");
            return;
        }
        
        // Combine all predictions
        Dataset<Row> combinedPredictions = allPredictions.get(0);
        for (int i = 1; i < allPredictions.size(); i++) {
            combinedPredictions = combinedPredictions.union(allPredictions.get(i));
        }
        
        // Create ensemble prediction based on whether it's classification or regression
        Dataset<Row> ensemblePredictions;
        
        if (isClassification) {
            // For classification, use majority voting
            ensemblePredictions = combinedPredictions
                    .groupBy("features", labelColumn)
                    .agg(
                            org.apache.spark.sql.functions.expr("approx_percentile(prediction, 0.5)").as("ensemble_prediction"),
                            org.apache.spark.sql.functions.count("prediction").as("node_count"),
                            org.apache.spark.sql.functions.collect_list("prediction").as("all_predictions")
                    );
        } else {
            // For regression, use average
            ensemblePredictions = combinedPredictions
                    .groupBy("features", labelColumn)
                    .agg(
                        org.apache.spark.sql.functions.avg("prediction").as("ensemble_prediction"),
                        org.apache.spark.sql.functions.stddev("prediction").as("prediction_std_dev"),
                        org.apache.spark.sql.functions.min("prediction").as("min_prediction"),
                        org.apache.spark.sql.functions.max("prediction").as("max_prediction"),
                        org.apache.spark.sql.functions.count("prediction").as("node_count")
                    )
                    .withColumn("prediction_range", 
                            org.apache.spark.sql.functions.col("max_prediction").minus(
                                org.apache.spark.sql.functions.col("min_prediction")));
        }
        
        // Add error calculations
        ensemblePredictions = ensemblePredictions
                .withColumn("error", 
                        org.apache.spark.sql.functions.col("ensemble_prediction").minus(
                            org.apache.spark.sql.functions.col(labelColumn)))
                .withColumn("abs_error", 
                        org.apache.spark.sql.functions.abs(
                            org.apache.spark.sql.functions.col("error")));
        
        // Save combined and ensemble predictions
        combinedPredictions
                .coalesce(1)
                .write()
                .option("header", "true")
                .mode("overwrite")
                .csv(outputFolder + "/all_node_predictions");
        
        ensemblePredictions
                .coalesce(1)
                .write()
                .option("header", "true")
                .mode("overwrite")
                .csv(outputFolder + "/ensemble_predictions");
        
        // Calculate ensemble performance metrics
        double ensembleRMSE = Math.sqrt(
                ensemblePredictions.select(org.apache.spark.sql.functions.avg(
                    org.apache.spark.sql.functions.pow("error", 2)))
                        .first().getDouble(0));
        
        double ensembleMAE = ensemblePredictions.select(org.apache.spark.sql.functions.avg("abs_error"))
                .first().getDouble(0);
        
        double totalSS = ensemblePredictions.select(
                org.apache.spark.sql.functions.sum(org.apache.spark.sql.functions.pow(
                    org.apache.spark.sql.functions.col(labelColumn)
                        .minus(org.apache.spark.sql.functions.avg(labelColumn)), 2)))
                .first().getDouble(0);
        
        double residualSS = ensemblePredictions.select(
                org.apache.spark.sql.functions.sum(org.apache.spark.sql.functions.pow("error", 2)))
                .first().getDouble(0);
        
        double ensembleR2 = 1 - (residualSS / totalSS);
        
        // Calculate additional classification metrics if applicable
        StringBuilder ensembleMetrics = new StringBuilder();
        ensembleMetrics.append("Ensemble Model Performance Metrics:\n");
        
        if (isClassification) {
            // For classification, calculate accuracy, precision, recall, F1
            Dataset<Row> confusionMatrix = ensemblePredictions
                    .groupBy(labelColumn)
                    .pivot("ensemble_prediction")
                    .count()
                    .na().fill(0);
            
            // Save confusion matrix
            confusionMatrix
                    .coalesce(1)
                    .write()
                    .option("header", "true")
                    .mode("overwrite")
                    .csv(outputFolder + "/ensemble_confusion_matrix");
            
            // Add accuracy calculation
            long correctPredictions = ensemblePredictions
                    .filter(org.apache.spark.sql.functions.col("ensemble_prediction")
                            .equalTo(org.apache.spark.sql.functions.col(labelColumn)))
                    .count();
            
            double accuracy = (double) correctPredictions / ensemblePredictions.count();
            ensembleMetrics.append("Accuracy: ").append(accuracy).append("\n");
        }
        
        // Add regression metrics
        ensembleMetrics.append("Root Mean Squared Error (RMSE): ").append(ensembleRMSE).append("\n");
        ensembleMetrics.append("Mean Absolute Error (MAE): ").append(ensembleMAE).append("\n");
        ensembleMetrics.append("R²: ").append(ensembleR2).append("\n");
        ensembleMetrics.append("Number of component models: ").append(allPredictions.size()).append("\n");
        
        Files.write(Paths.get(outputFolder + "/ensemble_performance.txt"), 
                ensembleMetrics.toString().getBytes());
        
        System.out.println("Combined predictions from " + allPredictions.size() + " nodes and created ensemble");
    }

    /**
     * Combines performance metrics from all nodes
     */
    private void combinePerformanceMetrics() throws IOException {
        StringBuilder combinedMetrics = new StringBuilder();
        combinedMetrics.append("Combined Performance Metrics from All Nodes\n");
        combinedMetrics.append("==============================================\n\n");
        
        Map<String, List<Double>> metricValues = new HashMap<String, List<Double>>();
        metricValues.put("RMSE", new ArrayList<Double>());
        metricValues.put("MSE", new ArrayList<Double>());
        metricValues.put("R²", new ArrayList<Double>());
        metricValues.put("MAE", new ArrayList<Double>());
        
        // if (isClassification) {
        //     metricValues.put("Accuracy", new ArrayList<Double>());
        //     metricValues.put("Precision", new ArrayList<Double>());
        //     metricValues.put("Recall", new ArrayList<Double>());
        //     metricValues.put("F1 Score", new ArrayList<Double>());
        // }
        
        for (int i = 0; i < nodesFolder.length; i++) {
            String nodeFolder = nodesFolder[i];
            Path metricsPath = Paths.get(nodeFolder + "/model_performance.txt");
            if (Files.exists(metricsPath)) {
                String nodeName = Paths.get(nodeFolder).getFileName().toString();
                combinedMetrics.append("Node: ").append(nodeName).append("\n");
                
                List<String> lines = Files.readAllLines(metricsPath);
                StringBuilder content = new StringBuilder();
                for (String line : lines) {
                    content.append(line).append("\n");
                }
                combinedMetrics.append(content.toString()).append("\n\n");
                
                // Extract metric values for averaging
                extractMetricValue(content.toString(), "Root Mean Squared Error (RMSE):", metricValues.get("RMSE"));
                extractMetricValue(content.toString(), "Mean Squared Error (MSE):", metricValues.get("MSE"));
                extractMetricValue(content.toString(), "R²:", metricValues.get("R²"));
                extractMetricValue(content.toString(), "Mean Absolute Error (MAE):", metricValues.get("MAE"));
                
                // if (isClassification) {
                //     extractMetricValue(content.toString(), "Accuracy:", metricValues.get("Accuracy"));
                //     extractMetricValue(content.toString(), "Precision:", metricValues.get("Precision"));
                //     extractMetricValue(content.toString(), "Recall:", metricValues.get("Recall"));
                //     extractMetricValue(content.toString(), "F1 Score:", metricValues.get("F1 Score"));
                // }
            }
        }
        
        // Calculate average metrics
        combinedMetrics.append("Average Performance Metrics Across All Nodes\n");
        combinedMetrics.append("==============================================\n");
        
        for (Map.Entry<String, List<Double>> entry : metricValues.entrySet()) {
            List<Double> values = entry.getValue();
            if (!values.isEmpty()) {
                double sum = 0.0;
                for (Double value : values) {
                    sum += value;
                }
                double average = sum / values.size();
                
                combinedMetrics.append("Average ").append(entry.getKey()).append(": ")
                        .append(average).append("\n");
            }
        }
        
        Files.write(Paths.get(outputFolder + "/combined_performance_metrics.txt"), 
                combinedMetrics.toString().getBytes());
    }
    
    /**
     *---------- Extract numeric value from a line containing a metric
     */
    private void extractMetricValue(String content, String metricPrefix, List<Double> values) {
        String[] lines = content.split("\n");
        for (int i = 0; i < lines.length; i++) {
            String line = lines[i].trim();
            if (line.startsWith(metricPrefix)) {
                try {
                    double value = Double.parseDouble(line.substring(line.indexOf(":") + 1).trim());
                    values.add(value);
                } catch (Exception e) {
                }
                break;
            }
        }
    }

    /**
     * Combines feature importance scores from all nodes
     */
    private void combineFeatureImportances() throws IOException {
        Map<String, List<Double>> featureImportances = new HashMap<String, List<Double>>();
        
        for (int n = 0; n < nodesFolder.length; n++) {
            String nodeFolder = nodesFolder[n];
            Path importancesPath = Paths.get(nodeFolder + "/feature_importances.txt");
            if (Files.exists(importancesPath)) {
                List<String> lines = Files.readAllLines(importancesPath);
                
                for (int i = 0; i < lines.size(); i++) {
                    String line = lines.get(i);
                    if (line.contains(":") && !line.startsWith("Feature Importances:")) {
                        String[] parts = line.split(":", 2);
                        if (parts.length == 2) {
                            String feature = parts[0].trim();
                            try {
                                double importance = Double.parseDouble(parts[1].trim());
                                
                                if (!featureImportances.containsKey(feature)) {
                                    featureImportances.put(feature, new ArrayList<Double>());
                                }
                                featureImportances.get(feature).add(importance);
                            } catch (NumberFormatException e) {
                                // Skip if parsing fails
                            }
                        }
                    }
                }
            }
        }
        
        // Calculate average importances
        StringBuilder avgImportances = new StringBuilder();
        avgImportances.append("Average Feature Importances Across All Nodes:\n");
        avgImportances.append("==============================================\n");
        
        // Convert to list for sorting
        List<Map.Entry<String, Double>> sortedFeatures = new ArrayList<Map.Entry<String, Double>>();
        for (Map.Entry<String, List<Double>> entry : featureImportances.entrySet()) {
            List<Double> values = entry.getValue();
            double sum = 0.0;
            for (Double value : values) {
                sum += value;
            }
            double avgImportance = sum / values.size();
            sortedFeatures.add(new java.util.AbstractMap.SimpleEntry<String, Double>(entry.getKey(), avgImportance));
        }
        
        // Sort by importance (descending)
        Collections.sort(sortedFeatures, new Comparator<Map.Entry<String, Double>>() {
            @Override
            public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });
        
        for (Map.Entry<String, Double> entry : sortedFeatures) {
            avgImportances.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        }
        
        Files.write(Paths.get(outputFolder + "/average_feature_importances.txt"), 
                avgImportances.toString().getBytes());
        
        System.out.println("Combined feature importances from all nodes");
        
        // Create a feature importance bar chart using Spark
        try {
            if (!sortedFeatures.isEmpty()) {
                // Convert sorted features to a dataset for visualization
                List<Row> rows = new ArrayList<>();
                for (Map.Entry<String, Double> entry : sortedFeatures) {
                    rows.add(org.apache.spark.sql.RowFactory.create(entry.getKey(), entry.getValue()));
                }
                
                org.apache.spark.sql.types.StructType schema = new org.apache.spark.sql.types.StructType(
                        new org.apache.spark.sql.types.StructField[] {
                            org.apache.spark.sql.types.DataTypes.createStructField("feature", 
                                    org.apache.spark.sql.types.DataTypes.StringType, false),
                            org.apache.spark.sql.types.DataTypes.createStructField("importance", 
                                    org.apache.spark.sql.types.DataTypes.DoubleType, false)
                        });
                
                Dataset<Row> featuresDF = spark.createDataFrame(rows, schema);
                
                // Save as CSV for later visualization
                featuresDF
                    .coalesce(1)
                    .write()
                    .option("header", "true")
                    .mode("overwrite")
                    .csv(outputFolder + "/feature_importance_data");
            }
        } catch (Exception e) {
            System.err.println("Error creating feature importance visualization: " + e.getMessage());
        }
    }

    /**
     * Aggregates model information from all nodes
     */
    private void aggregateModelInformation() throws IOException {
        StringBuilder modelInfo = new StringBuilder();
        modelInfo.append("Aggregated Model Information from All Nodes\n");
        modelInfo.append("==============================================\n\n");
        
        // Aggregate key model parameters
        Map<String, List<String>> modelParams = new HashMap<>();
        
        for (int i = 0; i < nodesFolder.length; i++) {
            String nodeFolder = nodesFolder[i];
            Path infoPath = Paths.get(nodeFolder + "/model_info.txt");
            if (Files.exists(infoPath)) {
                String nodeName = Paths.get(nodeFolder).getFileName().toString();
                modelInfo.append("Node: ").append(nodeName).append("\n");
                modelInfo.append("--------------------\n");
                
                // Extract model info
                List<String> infoLines = Files.readAllLines(infoPath);
                for (int j = 0; j < infoLines.size(); j++) {
                    String line = infoLines.get(j);
                    modelInfo.append(line).append("\n");
                    
                    // Track key parameters
                    if (line.contains(":")) {
                        String[] parts = line.split(":", 2);
                        if (parts.length == 2) {
                            String paramName = parts[0].trim();
                            String paramValue = parts[1].trim();
                            
                            if (!modelParams.containsKey(paramName)) {
                                modelParams.put(paramName, new ArrayList<>());
                            }
                            modelParams.get(paramName).add(paramValue);
                        }
                    }
                }
                modelInfo.append("\n");
            }
        }
        
        // Add summary of model parameters
        modelInfo.append("Common Model Parameters:\n");
        modelInfo.append("==============================================\n");
        
        for (Map.Entry<String, List<String>> entry : modelParams.entrySet()) {
            List<String> values = entry.getValue();
            if (!values.isEmpty()) {
                Map<String, Integer> valueCounts = new HashMap<>();
                
                // Count occurrences of each value
                for (String value : values) {
                    valueCounts.put(value, valueCounts.getOrDefault(value, 0) + 1);
                }
                
                // Find most common value
                String mostCommonValue = null;
                int maxCount = 0;
                for (Map.Entry<String, Integer> vc : valueCounts.entrySet()) {
                    if (vc.getValue() > maxCount) {
                        maxCount = vc.getValue();
                        mostCommonValue = vc.getKey();
                    }
                }
                
                modelInfo.append(entry.getKey()).append(": ").append(mostCommonValue)
                        .append(" (used in ").append(maxCount).append(" of ")
                        .append(nodesFolder.length).append(" nodes)\n");
            }
        }
        
        Files.write(Paths.get(outputFolder + "/aggregated_model_info.txt"), 
                modelInfo.toString().getBytes());
        
        System.out.println("Aggregated model information from all nodes");
    }
    
    /**
     * Combines decision trees from all nodes into an ensemble model
     */
    private void combineDecisionTrees() throws IOException {
        StringBuilder treeInfo = new StringBuilder();
        treeInfo.append("Decision Tree Aggregation Information\n");
        treeInfo.append("=====================================\n\n");
        
        int totalTreesFound = 0;
        List<String> allTreePaths = new ArrayList<>();
        
        // Find all tree models
        for (String nodeFolder : nodesFolder) {
            for (int i = 0; i < 10; i++) { // Looking for trees 0-9
                String treePath = nodeFolder + "/trees/tree_" + i+".txt";
                Path path = Paths.get(treePath);
                // if (Files.exists(path) && Files.isDirectory(path)) {
                //     System.out.println("Found tree at: " + treePath);
                    totalTreesFound++;
                    allTreePaths.add(treePath);
                // }
            }
        }
        
        treeInfo.append("Found ").append(totalTreesFound).append(" decision trees across ")
                .append(nodesFolder.length).append(" nodes.\n\n");
        
        if (totalTreesFound == 0) {
            treeInfo.append("No decision trees found to aggregate.\n");
            Files.write(Paths.get(outputFolder + "/tree_aggregation_info.txt"), 
                    treeInfo.toString().getBytes());
            return;
        }
        
        // Create directory structure for aggregated model
        Path aggModelPath = Paths.get(outputFolder + "/aggregated_model");
        Files.createDirectories(aggModelPath);
        
        // Copy trees to aggregated model directory with new indices
        for (int i = 0; i < allTreePaths.size(); i++) {
            String sourcePath = allTreePaths.get(i);
            String targetPath = aggModelPath.toString() + "/tree_" + i;
            
            treeInfo.append("Copying tree from: ").append(sourcePath)
                    .append(" to: ").append(targetPath).append("\n");
            
            // Create target directory
            Files.createDirectories(Paths.get(targetPath));
            
            // Copy all files from source tree to target
            copyDirectory(Paths.get(sourcePath), Paths.get(targetPath));
        }
        
        // Create metadata for the aggregated model
        StringBuilder metadata = new StringBuilder();
        metadata.append("{\n");
        metadata.append("  \"class\": \"").append(isClassification ? 
                "org.apache.spark.ml.classification.RandomForestClassificationModel" : 
                "org.apache.spark.ml.regression.RandomForestRegressionModel").append("\",\n");
        metadata.append("  \"timestamp\": ").append(System.currentTimeMillis()).append(",\n");
        metadata.append("  \"sparkVersion\": \"3.3.0\",\n");
        metadata.append("  \"uid\": \"rf_aggregated_model\",\n");
        metadata.append("  \"paramMap\": {\n");
        metadata.append("    \"numTrees\": ").append(totalTreesFound).append(",\n");
        metadata.append("    \"featureSubsetStrategy\": \"auto\",\n");
        metadata.append("    \"impurity\": \"variance\",\n");
        metadata.append("    \"maxDepth\": 20,\n");
        metadata.append("    \"maxBins\": 32,\n");
        metadata.append("    \"minInfoGain\": 0.0,\n");
        metadata.append("    \"minInstancesPerNode\": 1,\n");
        metadata.append("    \"seed\": 12345,\n");
        metadata.append("    \"subsamplingRate\": 1.0\n");
        metadata.append("  }\n");
        metadata.append("}\n");
        
        Files.write(Paths.get(aggModelPath.toString() + "/metadata"), 
                metadata.toString().getBytes());
        
        treeInfo.append("\nCreated aggregated model with ").append(totalTreesFound)
                .append(" trees at: ").append(aggModelPath.toString()).append("\n");
        treeInfo.append("\nNote: This aggregated model is a structural combination of trees and may require\n");
        treeInfo.append("additional processing to be used directly with Spark MLlib.\n");
        
        Files.write(Paths.get(outputFolder + "/tree_aggregation_info.txt"), 
                treeInfo.toString().getBytes());
        
        System.out.println("Combined " + totalTreesFound + " decision trees into aggregated model");
    }
    
    /**
     * Utility method to copy a directory
     */
    private void copyDirectory(Path source, Path target) throws IOException {
        Files.walkFileTree(source, new java.nio.file.SimpleFileVisitor<Path>() {
            @Override
            public java.nio.file.FileVisitResult preVisitDirectory(Path dir, java.nio.file.attribute.BasicFileAttributes attrs) 
                    throws IOException {
                Path targetDir = target.resolve(source.relativize(dir));
                try {
                    Files.copy(dir, targetDir);
                } catch (IOException e) {
                    if (!Files.exists(targetDir)) {
                        throw e;
                    }
                }
                return java.nio.file.FileVisitResult.CONTINUE;
            }
            
            @Override
            public java.nio.file.FileVisitResult visitFile(Path file, java.nio.file.attribute.BasicFileAttributes attrs) 
                    throws IOException {
                Files.copy(file, target.resolve(source.relativize(file)), 
                        java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                return java.nio.file.FileVisitResult.CONTINUE;
            }
        });
    }


    /**
     * Utility method to get all folders in a directory
     */
    public static String[] getAllFolders(String directoryPath) {
        File directory = new File(directoryPath);
        
        // Check if the directory exists
        if (!directory.exists() || !directory.isDirectory()) {
            System.err.println("Invalid directory path: " + directoryPath);
            return new String[0];
        }
        
        // Get all files and directories in the specified path
        File[] files = directory.listFiles();
        
        if (files == null) {
            System.err.println("Error reading directory contents: " + directoryPath);
            return new String[0];
        }
        
        // Count directories
        List<String> folderNames = new ArrayList<>();
        for (File file : files) {
            if (file.isDirectory()) {
                folderNames.add(file.getName());
            }
        }
        
        // Convert list to array
        String[] nodesFolder = folderNames.toArray(new String[0]);
        
        System.out.println("Found " + nodesFolder.length + " folders in " + directoryPath);
        return nodesFolder;
    }

}




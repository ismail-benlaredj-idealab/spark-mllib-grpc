package io.grpc.analytics;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;

public class LinearRegressionAgg {
    private static final String FEATURE_IMPORTANCE_DIR = "feature_importance";
    
    private LinearRegressionAgg() {} 

    private static class SumAndCount {
        double sum = 0.0;
        int count = 0;
        void add(double value) { this.sum += value; this.count++; }
        double getAverage() { return (count == 0) ? 0.0 : sum / count; }
    }

    /**
     * Calculate averages from a list of input files and save to output file
     * 
     * @param inputFilePaths List of paths to input files
     * @param outputFilePath Path where to save the output file
     * @return true if successful
     * @throws IOException if there's an issue with file operations
     */
    public static boolean calculateAndSaveAverages(List<String> inputFilePaths, String outputFilePath)
            throws IOException {

        if (inputFilePaths == null || inputFilePaths.isEmpty()) {
            throw new IllegalArgumentException("Input file paths list cannot be empty");
        }

        Map<String, SumAndCount> featureAggregates = performCalculation(inputFilePaths);

        Map<String, Double> averageResults = new LinkedHashMap<>(); 
        List<String> sortedFeatures = new ArrayList<>(featureAggregates.keySet());
        Collections.sort(sortedFeatures);
        for(String feature : sortedFeatures) {
             SumAndCount sc = featureAggregates.get(feature);
             if (sc != null) {
                 averageResults.put(feature, sc.getAverage());
             }
        }

        writeAveragesToFile(averageResults, outputFilePath); 
        
        return true; 
    }

    /**
     * Aggregates feature importance results across multiple nodes and datasets
     * 
     * @param baseDirectory The root directory containing all node folders
     * @param datasetPrefix The prefix used to identify relevant datasets (e.g., "dataset1")
     * @param outputFilePath Path where to save the aggregated results
     * @return true if successful
     * @throws IOException if there's an issue with file operations
     */
    public static boolean aggregateNodeResults(String baseDirectory, String datasetPrefix, String outputFilePath) 
            throws IOException {
        
        Path basePath = Paths.get(baseDirectory);
        if (!Files.isDirectory(basePath)) {
            throw new IllegalArgumentException("Not a valid directory: " + baseDirectory);
        }
        
        System.out.println("Searching for feature importance files in " + baseDirectory);
        System.out.println("Looking for datasets with prefix: " + datasetPrefix);
        
        // Find all node directories
        List<Path> nodePaths = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(basePath, 
                path -> Files.isDirectory(path))) {
            for (Path entry : stream) {
                nodePaths.add(entry);
            }
        }
        
        if (nodePaths.isEmpty()) {
            System.out.println("No node directories found in " + baseDirectory);
            return false;
        }
        
        System.out.println("Found " + nodePaths.size() + " node directories");
        
        // Collect all relevant feature importance CSV files
        List<String> featureImportanceFiles = new ArrayList<>();
        for (Path nodePath : nodePaths) {
            String nodeName = nodePath.getFileName().toString();
            System.out.println("Searching in node: " + nodeName);
            
            // Find dataset directories that match the prefix
            try (DirectoryStream<Path> datasetStream = Files.newDirectoryStream(nodePath, 
                    path -> Files.isDirectory(path) && 
                           path.getFileName().toString().startsWith(datasetPrefix))) {
                
                for (Path datasetPath : datasetStream) {
                    String datasetName = datasetPath.getFileName().toString();
                    System.out.println("  Found dataset: " + datasetName);
                    
                    // Look for feature_importance directory
                    Path featureImportancePath = datasetPath.resolve(FEATURE_IMPORTANCE_DIR);
                    if (Files.exists(featureImportancePath) && Files.isDirectory(featureImportancePath)) {
                        // Find all CSV files
                        try (DirectoryStream<Path> csvStream = Files.newDirectoryStream(featureImportancePath, 
                                path -> path.toString().endsWith(".csv") && Files.isRegularFile(path))) {
                            
                            for (Path csvPath : csvStream) {
                                System.out.println("    Found CSV: " + csvPath);
                                featureImportanceFiles.add(csvPath.toString());
                            }
                        }
                    } else {
                        System.out.println("    No feature_importance directory found in " + datasetName);
                    }
                }
            }
        }
        
        if (featureImportanceFiles.isEmpty()) {
            System.out.println("No feature importance CSV files found matching the criteria");
            return false;
        }
        
        System.out.println("Total feature importance files found: " + featureImportanceFiles.size());
        
        // Process the files using the existing method
        return calculateAndSaveAverages(featureImportanceFiles, outputFilePath);
    }

    private static Map<String, SumAndCount> performCalculation(List<String> filePaths) throws IOException {
        Map<String, SumAndCount> aggregates = new HashMap<>();

        if (filePaths.isEmpty()) {
             return Collections.emptyMap(); 
        }
        System.out.println("Processing " + filePaths.size() + " input files");

        for (String filePath : filePaths) {
            Path path = Paths.get(filePath);
            if (Files.exists(path) && Files.isRegularFile(path)) {
                processSingleFile(path, aggregates);
            } else {
                System.err.println("Warning: File does not exist or is not a regular file: " + filePath);
            }
        }
        return aggregates;
    }

    private static void processSingleFile(Path filePath, Map<String, SumAndCount> aggregates) {
        System.out.println(" -> Processing input file: " + filePath.getFileName()); 
        try (BufferedReader reader = Files.newBufferedReader(filePath)) {
            String line;
            String header = reader.readLine(); // Read/skip header
            if (header == null) { 
                System.err.println("Warning: File " + filePath.getFileName() + " is empty or header-only."); 
                return; 
            }

            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) continue;
                String[] parts = line.split(","); 
                if (parts.length == 2) {
                    String feature = parts[0].trim();
                    try {
                        double coefficient = Double.parseDouble(parts[1].trim());
                        SumAndCount sc = aggregates.computeIfAbsent(feature, k -> new SumAndCount());
                        sc.add(coefficient);
                    } catch (NumberFormatException e) {
                        System.err.println("Error parsing number in file " + filePath.getFileName() + ": " + parts[1]);
                    }
                } else {
                    System.err.println("Invalid line format in file " + filePath.getFileName() + ": " + line);
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading file " + filePath.getFileName() + ": " + e.getMessage());
        }
    }
    
    private static void writeAveragesToFile(Map<String, Double> averagesMap, String filePath) throws IOException {
        System.out.println(" -> Saving average coefficients to: " + filePath);
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(filePath), 
                                                              StandardOpenOption.CREATE, 
                                                              StandardOpenOption.TRUNCATE_EXISTING)) {
            writer.write("Feature,AverageCoefficient");
            writer.newLine();
            for (Map.Entry<String, Double> entry : averagesMap.entrySet()) {
                String line = String.format("%s,%.15g", entry.getKey(), entry.getValue());
                writer.write(line);
                writer.newLine();
            }
        } 
        System.out.println(" -> Save complete.");
    }

    /**
     * Original method for backward compatibility
     */
    public static boolean calculateAndSaveAverages(String inputDirectoryPath, String inputFilePrefix, String outputFilePath)
            throws IOException, IllegalArgumentException {

        Path inputDir = Paths.get(inputDirectoryPath);
        if (!Files.isDirectory(inputDir)) {
            throw new IllegalArgumentException("path is not a valid: " + inputDirectoryPath);
        }

        List<Path> filesToProcess = findFiles(inputDir, inputFilePrefix);
        List<String> filePathStrings = new ArrayList<>();
        for (Path path : filesToProcess) {
            filePathStrings.add(path.toString());
        }
        
        return calculateAndSaveAverages(filePathStrings, outputFilePath);
    }

    private static List<Path> findFiles(Path directory, String prefix) throws IOException {
        List<Path> foundFiles = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory, 
                                            path -> path.getFileName().toString().startsWith(prefix) 
                                                    && Files.isRegularFile(path))) {
            for (Path entry : stream) {
                foundFiles.add(entry);
            }
        } 
        return foundFiles;
    }
}
package io.grpc.analytics;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;

public class LinearRegressionAgg {
    private LinearRegressionAgg() {} 


    private static class SumAndCount {
        double sum = 0.0;
        int count = 0;
        void add(double value) { this.sum += value; this.count++; }
        double getAverage() { return (count == 0) ? 0.0 : sum / count; }
    }


    public static boolean calculateAndSaveAverages(String inputDirectoryPath, String inputFilePrefix, String outputFilePath)
            throws IOException, IllegalArgumentException {

        Path inputDir = Paths.get(inputDirectoryPath);
        if (!Files.isDirectory(inputDir)) {
            throw new IllegalArgumentException("path is not a valid: " + inputDirectoryPath);
        }

        Map<String, SumAndCount> featureAggregates = performCalculation(inputDir, inputFilePrefix);

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

    private static Map<String, SumAndCount> performCalculation(Path directory, String prefix) throws IOException {
        Map<String, SumAndCount> aggregates = new HashMap<>();
        List<Path> filesToProcess = findFiles(directory, prefix);

        if (filesToProcess.isEmpty()) {
             return Collections.emptyMap(); 
        }
        System.out.println("Found " + filesToProcess.size() + " input files to process in " + directory);

        for (Path filePath : filesToProcess) {
            processSingleFile(filePath, aggregates);
        }
        return aggregates;
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
                    }
                } else {
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

}

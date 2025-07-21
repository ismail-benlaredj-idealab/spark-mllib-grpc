package io.grpc.analytics;

import io.grpc.stub.StreamObserver;
import io.grpc.Status;
// This import is assumed to exist for the gRPC generated message class.
import io.grpc.analytics.DatasetResult; 

import java.util.*;
import java.io.*;
import java.nio.file.*;

public class AnonymizationAccuracyService extends AnonymizationAccuracyGrpc.AnonymizationAccuracyImplBase {


    private static class LoadedData {
        final List<List<String>> originalData;
        final List<List<String>> anonymizedData;
        final List<String> columnNames;

        LoadedData(List<List<String>> originalData, List<List<String>> anonymizedData, List<String> columnNames) {
            this.originalData = originalData;
            this.anonymizedData = anonymizedData;
            this.columnNames = columnNames;
        }
    }

    @Override
    public void calculateECS(RequestAnonymizationAccuracy req,
                             StreamObserver<ResponseAnonymizationAccuracy> responseObserver) {
        try {
            String originalPath = req.getOriginalDatasetPath();
            String anonymizedPath = req.getAnonymizedDatasetPath();
            List<String> quasiIdentifierNames = req.getQuasiIdentifierNamesList();

            // Validate input paths
            if (!Files.exists(Paths.get(originalPath))) {
                throw new FileNotFoundException("Original dataset not found: " + originalPath);
            }
            if (!Files.exists(Paths.get(anonymizedPath))) {
                throw new FileNotFoundException("Anonymized dataset not found: " + anonymizedPath);
            }

            // Load data from CSV files
            LoadedData dataResult = loadDataFromCSV(originalPath, anonymizedPath);

            // Get column indices for quasi-identifiers
            List<Integer> quasiIdentifiers = getColumnIndices(dataResult.columnNames, quasiIdentifierNames);

            if (quasiIdentifiers.isEmpty()) {
                throw new IllegalArgumentException("No valid quasi-identifiers found in the dataset");
            }

            // Calculate ECS score
            double ecsScore = calculateECS(dataResult.originalData, dataResult.anonymizedData, quasiIdentifiers);

            // Build response
            ResponseAnonymizationAccuracy response = ResponseAnonymizationAccuracy.newBuilder()
                    .setEcsScore(ecsScore)
                    .setStatus("SUCCESS")
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            System.err.println("Error calculating ECS: " + e.getMessage());
            e.printStackTrace();

            ResponseAnonymizationAccuracy errorResponse = ResponseAnonymizationAccuracy.newBuilder()
                    .setEcsScore(0.0)
                    .setStatus("ERROR")
                    .setErrorMessage("Error calculating ECS: " + e.getMessage())
                    .build();

            responseObserver.onNext(errorResponse);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void calculateBatchECS(RequestBatchAnonymizationAccuracy req,
                                  StreamObserver<ResponseBatchAnonymizationAccuracy> responseObserver) {
        try {
            // String datasetBaseName = req.getDatasetBaseName();
            int numDatasets = req.getNumDatasets();
            String originalBasePath = req.getOriginalDatasetBasePath();
            String anonymizedBasePath = req.getAnonymizedDatasetBasePath();
            List<String> quasiIdentifierNames = req.getQuasiIdentifierNamesList();
            String outputPath = req.getOutputPath();

            // Create output directory if it doesn't exist
            if (!outputPath.isEmpty()) {
                Files.createDirectories(Paths.get(outputPath));
            }

            // This list now holds the gRPC message type 'DatasetResult'
            List<DatasetResult> results = new ArrayList<>();
            List<Double> ecsScores = new ArrayList<>();

            for (int i = 1; i <= numDatasets; i++) {
                // Use the builder for the gRPC message 'DatasetResult'
                DatasetResult.Builder resultBuilder = DatasetResult.newBuilder().setDatasetNum(i);

                try {
                    // Correctly construct file paths
                    String originalCsvPath = Paths.get(originalBasePath).toString();
                    String anonymizedCsvPath = Paths.get(anonymizedBasePath).toString();

                    System.out.println("Processing Dataset " + i + ": " + anonymizedCsvPath);

                    if (!Files.exists(Paths.get(originalCsvPath))) {
                        resultBuilder.setStatus("ERROR")
                                     .setErrorMessage("Original dataset not found: " + originalCsvPath)
                                     .setEcsScore(0.0);
                        results.add(resultBuilder.build());
                        continue;
                    }

                    if (!Files.exists(Paths.get(anonymizedCsvPath))) {
                        resultBuilder.setStatus("ERROR")
                                     .setErrorMessage("Anonymized dataset not found: " + anonymizedCsvPath)
                                     .setEcsScore(0.0);
                        results.add(resultBuilder.build());
                        continue;
                    }

                    // Load data using the helper class 'LoadedData'
                    LoadedData dataResult = loadDataFromCSV(originalCsvPath, anonymizedCsvPath);

                    // Get column indices for quasi-identifiers
                    List<Integer> quasiIdentifiers = getColumnIndices(dataResult.columnNames, quasiIdentifierNames);

                    if (quasiIdentifiers.isEmpty()) {
                        resultBuilder.setStatus("ERROR")
                                     .setErrorMessage("No valid quasi-identifiers found")
                                     .setEcsScore(0.0);
                        results.add(resultBuilder.build());
                        continue;
                    }

                    // Calculate ECS score
                    double ecsScore = calculateECS(dataResult.originalData, dataResult.anonymizedData, quasiIdentifiers);

                    resultBuilder.setStatus("SUCCESS").setEcsScore(ecsScore);
                    results.add(resultBuilder.build());
                    ecsScores.add(ecsScore);

                    System.out.printf("Dataset %d completed successfully. ECS Score: %.6f%n", i, ecsScore);

                } catch (Exception e) {
                    System.err.println("Error processing dataset " + i + ": " + e.getMessage());
                    resultBuilder.setStatus("ERROR")
                                 .setErrorMessage("Error processing dataset: " + e.getMessage())
                                 .setEcsScore(0.0);
                    results.add(resultBuilder.build());
                }
            }

            // Calculate statistics
            double stdEcs = 0.0, minEcs = 0.0, maxEcs = 0.0;
            final double meanEcs;
            if (!ecsScores.isEmpty()) {
                meanEcs = ecsScores.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
                if (ecsScores.size() > 1) {
                    double variance = ecsScores.stream()
                            .mapToDouble(score -> Math.pow(score - meanEcs, 2))
                            .sum() / ecsScores.size(); // Population variance
                    stdEcs = Math.sqrt(variance);
                }
                minEcs = ecsScores.stream().mapToDouble(Double::doubleValue).min().orElse(0.0);
                maxEcs = ecsScores.stream().mapToDouble(Double::doubleValue).max().orElse(0.0);
            } else {
                meanEcs = 0.0;
            }

            // Save results to CSV if output path is provided
            if (!outputPath.isEmpty()) {
                saveResultsToCSV(results, Paths.get(outputPath, "equivalent_class_size_results.csv").toString());
            }

            // Build response
            ResponseBatchAnonymizationAccuracy response = ResponseBatchAnonymizationAccuracy.newBuilder()
                    .addAllResults(results)
                    .setTotalDatasets(numDatasets)
                    .setSuccessfulDatasets(ecsScores.size())
                    .setFailedDatasets(numDatasets - ecsScores.size())
                    .setMeanEcsScore(meanEcs)
                    .setStdEcsScore(stdEcs)
                    .setMinEcsScore(minEcs)
                    .setMaxEcsScore(maxEcs)
                    .setStatus("SUCCESS")
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            System.err.println("Error in batch ECS calculation: " + e.getMessage());
            e.printStackTrace();

            ResponseBatchAnonymizationAccuracy errorResponse = ResponseBatchAnonymizationAccuracy.newBuilder()
                    .setTotalDatasets(0)
                    .setSuccessfulDatasets(0)
                    .setFailedDatasets(0)
                    .setStatus("ERROR")
                    .setErrorMessage("Error in batch processing: " + e.getMessage())
                    .build();

            responseObserver.onNext(errorResponse);
            responseObserver.onCompleted();
        }
    }

    // Helper methods from original AnonymizationAccuracy class (unchanged)
    private static boolean areEquivalent(List<String> t1, List<String> t2) {
        if (t1.size() != t2.size()) return false;
        for (int i = 0; i < t1.size(); i++) {
            if (!t1.get(i).equals(t2.get(i))) {
                return false;
            }
        }
        return true;
    }

    private static String tupleToString(List<String> tuple) {
        return String.join(",", tuple);
    }

    public static double computeECS(List<List<String>> anonymized) {
        Map<String, Integer> equivalenceClasses = new HashMap<>();

        for (List<String> tuple : anonymized) {
            String key = tupleToString(tuple);
            equivalenceClasses.put(key, equivalenceClasses.getOrDefault(key, 0) + 1);
        }

        double sumSquaredSizes = 0.0;
        for (int classSize : equivalenceClasses.values()) {
            sumSquaredSizes += Math.pow(classSize, 2);
        }

        return sumSquaredSizes / anonymized.size();
    }

    public double calculateECS(List<List<String>> originalDataset,
                               List<List<String>> anonymizedDataset,
                               List<Integer> quasiIdentifiers) {

        List<List<String>> anonymizedQI = new ArrayList<>();

        for (List<String> tuple : anonymizedDataset) {
            List<String> qiTuple = new ArrayList<>();
            for (int idx : quasiIdentifiers) {
                if (idx < tuple.size()) {
                    qiTuple.add(tuple.get(idx));
                }
            }
            anonymizedQI.add(qiTuple);
        }

        double ecsScore = computeECS(anonymizedQI);

        System.out.printf("Equivalent Class Size (ECS): %.6f%n", ecsScore);

        return ecsScore;
    }


    /**
     * Updated to return the new 'LoadedData' class.
     */
    private LoadedData loadDataFromCSV(String originalCsvPath, String anonymizedCsvPath) throws IOException {
        List<List<String>> originalData = new ArrayList<>();
        List<List<String>> anonymizedData = new ArrayList<>();
        List<String> columnNames = new ArrayList<>();

        try (BufferedReader reader = Files.newBufferedReader(Paths.get(originalCsvPath))) {
            String line = reader.readLine();
            if (line != null) {
                columnNames = new ArrayList<>(Arrays.asList(line.split(",")));
                for (int i = 0; i < columnNames.size(); i++) {
                    columnNames.set(i, columnNames.get(i).trim());
                }
            }

            while ((line = reader.readLine()) != null) {
                List<String> row = new ArrayList<>();
                // Use split with -1 limit to keep trailing empty strings
                String[] values = line.split(",", -1);
                for (String value : values) {
                    String trimmedValue = value.trim();
                    row.add(trimmedValue);
                }
                originalData.add(row);
            }
        }

        try (BufferedReader reader = Files.newBufferedReader(Paths.get(anonymizedCsvPath))) {
            reader.readLine(); // Skip header
            String line;
            while ((line = reader.readLine()) != null) {
                List<String> row = new ArrayList<>();
                // Use split with -1 limit to keep trailing empty strings
                String[] values = line.split(",", -1);
                for (String value : values) {
                    String trimmedValue = value.trim();
                    row.add(trimmedValue);
                }
                anonymizedData.add(row);
            }
        }

        return new LoadedData(originalData, anonymizedData, columnNames);
    }

    private List<Integer> getColumnIndices(List<String> columnNames, List<String> targetColumns) {
        List<Integer> indices = new ArrayList<>();
        for (String col : targetColumns) {
            int index = columnNames.indexOf(col);
            if (index != -1) {
                indices.add(index);
            }
        }
        return indices;
    }

    /**
     * Updated to take a List of the gRPC 'DatasetResult' message type.
     */
    private void saveResultsToCSV(List<DatasetResult> results, String outputFilename) throws IOException {
        try (PrintWriter writer = new PrintWriter(new FileWriter(outputFilename))) {
            writer.println("Quisi Identifier number,Anonymization Accuracy");

            for (DatasetResult result : results) {
                writer.printf("%d,%.6f%n",
                        result.getDatasetNum(),
                        result.getEcsScore());
            }
        }
        System.out.println("\nResults saved to " + outputFilename);
    }
}
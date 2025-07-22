package io.grpc.analytics;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.Arrays;

/**
 * Handles the core logic for calculating Anonymization Accuracy (AA).
 * This class reads datasets from server-local paths, computes the AA score,
 * and writes the result to a server-local file, matching the existing
 * architecture.
 */
public class AnonymizationAccuracyAnalytics {

    private final String originalCsvPath;
    private final String anonymizedCsvPath;
    private final List<String> quasiIdentifierNames;
    private final String outputResultsPath;

    public AnonymizationAccuracyAnalytics(String originalCsvPath, String anonymizedCsvPath,
            List<String> quasiIdentifierNames, String outputResultsPath) {
        this.originalCsvPath = originalCsvPath;
        this.anonymizedCsvPath = anonymizedCsvPath;
        this.quasiIdentifierNames = quasiIdentifierNames;
        this.outputResultsPath = outputResultsPath;
    }

    /**
     * Executes the full AA calculation process.
     * 
     * @return The calculated AA score.
     * @throws IOException if there is an error reading or writing files.
     */
    public double runCalculation() throws IOException {
        // 1. Load data from server-local CSV files
        DatasetResult data = loadDataFromCSV(originalCsvPath, anonymizedCsvPath);

        // 2. Get column indices for the specified quasi-identifiers
        List<Integer> quasiIdentifiers = getColumnIndices(data.columnNames, quasiIdentifierNames);
        if (quasiIdentifiers.isEmpty()) {
            throw new IllegalArgumentException("No valid quasi-identifiers found in the dataset header.");
        }

        // 3. Extract only the QI columns from the datasets
        List<List<String>> originalQI = extractQuasiIdentifiers(data.originalData, quasiIdentifiers);
        List<List<String>> anonymizedQI = extractQuasiIdentifiers(data.anonymizedData, quasiIdentifiers);

        // 4. Compute the AA score
        double aaScore = computeAA(originalQI, anonymizedQI);

        // 5. Save the result to the specified output path
        saveResults(quasiIdentifiers.size(), aaScore);

        return aaScore;
    }

    private void saveResults(int numQuasiIdentifiers, double score) throws IOException {
        File file = new File(this.outputResultsPath);
        boolean fileExists = file.exists();

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(this.outputResultsPath, true))) {
            if (!fileExists) {
                writer.write("num_quasi_identifiers,anonymization_accuracy_score\n");
            }
            writer.write(String.format("%d,%.6f%n", numQuasiIdentifiers, score));
        }
    }

    // --- The following methods are adapted from your original AA code ---

    private double computeAA(List<List<String>> original, List<List<String>> anonymized) {
        if (original.size() != anonymized.size()) {
            throw new IllegalArgumentException("Dataset sizes must match");
        }
        int n = original.size();
        if (n == 0)
            return 1.0; // If no records, accuracy is perfect

        double totalPrecision = 0.0;
        for (int i = 0; i < n; i++) {
            List<String> originalTuple = original.get(i);
            List<String> anonymizedTuple = anonymized.get(i);

            long anonymizedMatches = anonymized.stream()
                    .filter(anonTuple -> isIndistinguishable(anonymizedTuple, anonTuple))
                    .count();

            if (anonymizedMatches == 0)
                continue;

            long correctMatches = 0;
            for (int j = 0; j < n; j++) {
                if (isIndistinguishable(anonymizedTuple, anonymized.get(j)) &&
                        isIndistinguishable(originalTuple, original.get(j))) {
                    correctMatches++;
                }
            }
            totalPrecision += (double) correctMatches / anonymizedMatches;
        }
        return totalPrecision / n;
    }

    private boolean isIndistinguishable(List<String> t1, List<String> t2) {
        if (t1.size() != t2.size())
            return false;
        for (int i = 0; i < t1.size(); i++) {
            String val1 = t1.get(i);
            String val2 = t2.get(i);
            if (val1 == null && val2 == null)
                continue;
            if (val1 == null || val2 == null)
                return false;
            // Simple comparison logic
            if (!val1.trim().equalsIgnoreCase(val2.trim()))
                return false;
        }
        return true;
    }

    private List<List<String>> extractQuasiIdentifiers(List<List<String>> dataset, List<Integer> quasiIdentifiers) {
        return dataset.stream()
                .map(tuple -> quasiIdentifiers.stream()
                        .map(idx -> (idx >= 0 && idx < tuple.size()) ? tuple.get(idx) : null)
                        .collect(Collectors.toList()))
                .collect(Collectors.toList());
    }

    private List<Integer> getColumnIndices(List<String> columnNames, List<String> targetColumns) {
        return targetColumns.stream()
                .map(col -> columnNames.indexOf(col.trim()))
                .filter(index -> index != -1)
                .collect(Collectors.toList());
    }

    private DatasetResult loadDataFromCSV(String originalPath, String anonymizedPath) throws IOException {
        List<String> columnNames;
        List<List<String>> originalData;
        List<List<String>> anonymizedData;

        try (BufferedReader reader = Files.newBufferedReader(Paths.get(originalPath))) {
            String headerLine = reader.readLine();
            if (headerLine == null)
                throw new IOException("Original CSV is empty.");
            columnNames = parseCsvLine(headerLine);
            originalData = reader.lines().map(this::parseCsvLine).collect(Collectors.toList());
        }

        try (BufferedReader reader = Files.newBufferedReader(Paths.get(anonymizedPath))) {
            reader.readLine(); // Skip header
            anonymizedData = reader.lines().map(this::parseCsvLine).collect(Collectors.toList());
        }

        return new DatasetResult(originalData, anonymizedData, columnNames);
    }

    private List<String> parseCsvLine(String line) {
        // This simple parser assumes no commas within quoted fields.
        return new ArrayList<>(Arrays.asList(line.split(",")));
        // return new ArrayList<>(List.of(line.split(",")));
    }

    private static class DatasetResult {
        final List<List<String>> originalData;
        final List<List<String>> anonymizedData;
        final List<String> columnNames;

        DatasetResult(List<List<String>> o, List<List<String>> a, List<String> c) {
            this.originalData = o;
            this.anonymizedData = a;
            this.columnNames = c;
        }
    }
}
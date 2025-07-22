package io.grpc.analytics;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.ByteString;

public class ClientAgg {

    private static final Logger logger = Logger.getLogger(Client.class.getName());

    private final ManagedChannel channel;
    private final DatasetAccessGrpc.DatasetAccessBlockingStub blockingStubFP;
    private final LinearRegressionGrpc.LinearRegressionBlockingStub blockingStubLinearRegression;
    private final AnonymityServiceGrpc.AnonymityServiceBlockingStub blockingStubAA;

    /** Construct client connecting to server at {@code host:port}. */
    public ClientAgg(String host, int port) {
        channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext() // Note: For production, use proper authentication
                .build();
        blockingStubFP = DatasetAccessGrpc.newBlockingStub(channel);
        blockingStubLinearRegression = LinearRegressionGrpc.newBlockingStub(channel);
        blockingStubAA = AnonymityServiceGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public void getRemoteDatasets(String folderPath, String outputFolderPath) {
        // Create request with only the folder path parameter
        RequestDatasetAccess request = RequestDatasetAccess.newBuilder()
                .setFolderPath(folderPath)
                .build();

        try {
            // Ensure output directory exists
            File outputFolder = new File(outputFolderPath);
            if (!outputFolder.exists()) {
                outputFolder.mkdirs();
                logger.info("Created output directory: " + outputFolder.getAbsolutePath());
            }

            // Get the response from the server
            ResponseDatasetAccess response = blockingStubFP.remoteDataset(request);

            // Validate response
            if (response == null || response.getFilesList().isEmpty()) {
                logger.severe("No files received from server");
                return;
            }

            // Process each file from the response
            int filesSaved = 0;
            for (FileData fileData : response.getFilesList()) {
                String fileName = fileData.getFileName();
                ByteString content = fileData.getContent();

                if (content.isEmpty()) {
                    logger.warning("Empty content for file: " + fileName);
                    continue;
                }

                // Create output file
                File outputFile = new File(outputFolder, fileName);

                // Write file content
                try (FileOutputStream fos = new FileOutputStream(outputFile)) {
                    content.writeTo(fos);
                    filesSaved++;
                    logger.info("File saved successfully: " + outputFile.getAbsolutePath());
                } catch (IOException e) {
                    logger.log(Level.SEVERE, "Error writing file " + fileName + ": " + e.getMessage(), e);
                }
            }

            logger.info("Successfully saved " + filesSaved + " files to " + outputFolder.getAbsolutePath());

        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
        }
    }

    public void triggerAACalculation(String originalPath, String anonymizedPath, List<String> qiColumns,
            String resultsPath) {
        logger.info("Will try to trigger AA calculation...");
        AARequest request = AARequest.newBuilder()
                .setOriginalCsvPath(originalPath)
                .setAnonymizedCsvPath(anonymizedPath)
                .addAllQuasiIdentifierColumns(qiColumns)
                .setOutputResultsPath(resultsPath)
                .build();

        try {
            AAResponse response = blockingStubAA.calculateAA(request);
            if (response.getStatus() == AAResponse.Status.SUCCESS) {
                logger.info("✅ RPC Success: " + response.getMessage());
            } else {
                logger.severe("❌ RPC Error: " + response.getMessage());
            }
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
        }
    }

    public static void main(String[] args) throws Exception {

        String MODE = "dev"; // dev or prod

        if (MODE == "prod") {

            List<String> nodes = Arrays.asList("pe01-vm04", "pe01-vm05", "pe01-vm06",
                    "pe02-vm04", "pe02-vm05", "pe02-vm06");

            for (String node : nodes) {
                Client client = new Client(node, 50051);

                try {
                    String dataset_path = args.length > 0 ? args[0] : readSettings("DATASET_PATH");

                } finally {
                    client.shutdown();
                }
            }
        } else {
            ClientAgg ClientAgg = new ClientAgg("localhost", 50051);
            // ClientAgg.getRemoteDatasets("/home/ismail/grpc-java-examples-master/datasets",
            //         "/home/ismail/grpc-java-examples-master/received_files");
            String quasiIdentifierFile = "/home/ismail/grpc-java-examples-master/datasets/quasi_identifiers.dat";
            List<List<String>> allQuasiIdentifiers = readQuasiIdentifiersFromFile(quasiIdentifierFile);
            String resultsFile = "/home/ismail/grpc-java-examples-master/datasets/AARes.csv";

            for (int i = 2; i < 11; i++) {
                try {
                    System.out.println("Processing round " + (i ) + " with quasi-identifiers: "
                            + allQuasiIdentifiers.get(i));
                    String originalFile = "/home/ismail/grpc-java-examples-master/datasets/banking_synthetic_v1.csv";
                    String anonymizedFile = "/home/ismail/grpc-java-examples-master/datasets/anonymized_bank_A"+(i)+".csv";
                    List<String> quasiIdentifiers = allQuasiIdentifiers.get(i);

                    System.out.println("Processing round " + (i + 1) + " with quasi-identifiers: " + quasiIdentifiers);

                    // --- Trigger the new service and get the score ---
                    ClientAgg.triggerAACalculation(originalFile, anonymizedFile, quasiIdentifiers, resultsFile);


                } catch (Exception e) {
                    System.out.println("Error processing round " + (i + 1) + ": " + e.getMessage());
                    e.printStackTrace();
                }
            }

              ClientAgg.getRemoteDatasets("/home/ismail/grpc-java-examples-master/datasets/AARes.csv",
                    "/home/ismail/grpc-java-examples-master/received_files");

            ClientAgg.aggregateCSVFiles("/home/ismail/grpc-java-examples-master/received_files",
                            "/home/ismail/grpc-java-examples-master/datasets/agg_results.csv");

            // String rootDirectory = "/home/ismail/grpc-java-examples-master/LR";
            // String datasetPrefix = "anonymized_tcloseness";
            // String outputPath =
            // "/home/ismail/grpc-java-examples-master/anonymized_tcloseness_aggregated_features.csv";

            // boolean success = LinearRegressionAgg.aggregateNodeResults(
            // rootDirectory,
            // datasetPrefix,
            // outputPath);

            // if (success) {
            // System.out.println("Node feature importance aggregation completed
            // successfully!");
            // } else {
            // System.out.println("Node feature importance aggregation did not complete
            // successfully.");
            // }

        }
    }

    /*********************
     * UTILS
     *********************************************************************/
    public static String readSettings(String param) {
        String filePath = "src/main/java/io/grpc/analytics/resources/settings.dat";
        filePath = Paths.get(filePath).toAbsolutePath().toString();

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith(param + "=")) {
                    return line.split("=", 2)[1];
                }
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Error reading settings file", e);
        }
        return null;
    }

    public static List<List<String>> readQuasiIdentifiersFromFile(String filePath) throws IOException {
    List<List<String>> quasiSets = new ArrayList<>();
    try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
        String line;
        while ((line = reader.readLine()) != null) {
            line = line.trim();
            if (!line.isEmpty()) {
                List<String> identifiers = new ArrayList<>();
                String[] parts = line.split(",");
                for (String part : parts) {
                    identifiers.add(part.trim());
                }
                quasiSets.add(identifiers);
            }
        }
    }
    return quasiSets;
}



public void aggregateCSVFiles(String folderPath, String outputPath) throws IOException {
    File folder = new File(folderPath);
    File[] csvFiles = folder.listFiles((dir, name) -> name.toLowerCase().endsWith(".csv"));
    
    if (csvFiles == null || csvFiles.length == 0) {
        throw new IOException("No CSV files found in the specified folder");
    }
    
    List<String[]> aggregatedData = new ArrayList<>();
    String[] headers = null;
    int fileCount = 0;
    
    for (File file : csvFiles) {
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            int rowIndex = 0;
            
            while ((line = reader.readLine()) != null) {
                String[] values = line.split(",");
                
                if (rowIndex == 0) {
                    // Handle headers
                    if (headers == null) {
                        headers = values.clone();
                        // Initialize aggregated data structure
                        aggregatedData.add(headers);
                    }
                } else {
                    // Handle data rows
                    if (fileCount == 0) {
                        // First file: initialize with its values
                        String[] row = new String[values.length];
                        for (int i = 0; i < values.length - 1; i++) {
                            row[i] = values[i]; // Copy non-numeric columns as is
                        }
                        row[values.length - 1] = values[values.length - 1]; // Last column value
                        aggregatedData.add(row);
                    } else {
                        // Subsequent files: add to existing values
                        if (rowIndex < aggregatedData.size()) {
                            String[] existingRow = aggregatedData.get(rowIndex);
                            double existingValue = Double.parseDouble(existingRow[existingRow.length - 1]);
                            double newValue = Double.parseDouble(values[values.length - 1]);
                            existingRow[existingRow.length - 1] = String.valueOf(existingValue + newValue);
                        }
                    }
                }
                rowIndex++;
            }
        }
        fileCount++;
    }
    
    // Calculate averages (divide by number of files)
    for (int i = 1; i < aggregatedData.size(); i++) {
        String[] row = aggregatedData.get(i);
        double sum = Double.parseDouble(row[row.length - 1]);
        double average = sum / fileCount;
        row[row.length - 1] = String.format("%.6f", average);
    }
    
    // Write results to output file
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath))) {
        for (String[] row : aggregatedData) {
            writer.write(String.join(",", row));
            writer.newLine();
        }
    }
}


}




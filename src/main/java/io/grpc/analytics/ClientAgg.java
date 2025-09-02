package io.grpc.analytics;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.protobuf.ByteString;

public class ClientAgg {

    private static final Logger logger = Logger.getLogger(Client.class.getName());

    private final ManagedChannel channel;
    private final LinearRegressionGrpc.LinearRegressionBlockingStub blockingStubLinearRegression;
    private final AnonymityServiceGrpc.AnonymityServiceBlockingStub blockingStubAA;
    private final ClustringAnalysisGrpc.ClustringAnalysisBlockingStub blockingStubKMeans;
    private final DatasetAccessGrpc.DatasetAccessBlockingStub blockingStubDatasetAccess;

    /** Construct client connecting to server at {@code host:port}. */
    public ClientAgg(String host, int port) {
        channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext() // Note: For production, use proper authentication
                .build();
        blockingStubLinearRegression = LinearRegressionGrpc.newBlockingStub(channel);
        blockingStubAA = AnonymityServiceGrpc.newBlockingStub(channel);
        blockingStubKMeans = ClustringAnalysisGrpc.newBlockingStub(channel);
        blockingStubDatasetAccess = DatasetAccessGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public void getRemoteDatasets(String node, String folderPath, String outputFolderPath) {
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
            ResponseDatasetAccess response = blockingStubDatasetAccess.remoteDataset(request);

            // Validate response
            if (response == null || response.getFilesList().isEmpty()) {
                logger.severe("No files received from server");
                return;
            }

            // Process each file from the response
            int filesSaved = 0;
            for (FileData fileData : response.getFilesList()) {
                String fileName = node + "_" + fileData.getFileName();
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

    public void applyAnalytics(String datasetPath, String outputPath) {
        RequestClustringKmeans request = RequestClustringKmeans.newBuilder()
                .setDatasetPath(datasetPath)
                .setOutputPath(outputPath)
                .build();
        try {
            ResponseClustringKmeans response = blockingStubKMeans.clustringKmeansServer(request);
            System.out.println("Kmeans Response: " + response.toString());
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
        }
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
            ResponseDatasetAccess response = blockingStubDatasetAccess.remoteDataset(request);
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
                // Create output file with path
                File outputFile = new File(outputFolder, fileName);
                // Create parent directories if they don't exist
                File parentDir = outputFile.getParentFile();
                if (parentDir != null && !parentDir.exists()) {
                    if (!parentDir.mkdirs()) {
                        logger.warning("Failed to create directory: " + parentDir.getAbsolutePath());
                    } else {
                        logger.info("Created directory structure: " + parentDir.getAbsolutePath());
                    }
                }
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

    public static void main(String[] args) throws Exception {

        String MODE = "dev"; // dev or prod

        if (MODE == "prod") {

            // List<String> nodes = Arrays.asList("pe01-vm04", "pe01-vm05", "pe01-vm06",
            // "pe02-vm04", "pe02-vm05", "pe02-vm06");
            List<String> nodes = Arrays.asList("pe01-vm03", "pe01-vm06");

            // for (String node : nodes) {
            //     ClientAgg ClientAgg = new ClientAgg(node, 50051);

              //  try {
                //     String quasiIdentifierFile = "/home/pe01-vm05/Documents/spark-mllib-grpc-dev/datasets/quasi_identifiers.dat";
                //     List<List<String>> allQuasiIdentifiers = readQuasiIdentifiersFromFile(quasiIdentifierFile);
                //     String resultsFile = "/home/" + node + "/Documents/spark-mllib-grpc-dev/datasets/AARes.csv";

                //     for (int i = 2; i < 11; i++) {
                //         try {
                //             System.out.println("Processing round " + (i) + " with quasi-identifiers: "
                //                     + allQuasiIdentifiers.get(i));
                //             String originalFile = "/home/" + node
                //                     + "/Documents/spark-mllib-grpc-dev/datasets/banking_synthetic_v1.csv";
                //             String anonymizedFile = "/home/" + node
                //                     + "/Documents/spark-mllib-grpc-dev/datasets/anonymized_bank_A" + (i) + ".csv";

                //             // we -2 because we have the same index
                //             // for datasets A2, A4... A2 is the first dataset set but i=2 is the 4th line in
                //             // the qusi identifires
                //             List<String> quasiIdentifiers = allQuasiIdentifiers.get(i - 2);

                //             System.out.println(
                //                     "Processing round " + (i + 1) + " with quasi-identifiers: " + quasiIdentifiers);

                //             // --- Trigger the new service and get the score ---

                //             ClientAgg.triggerAACalculation(originalFile, anonymizedFile, quasiIdentifiers, resultsFile);

                //         } catch (Exception e) {
                //             System.out.println("Error processing round " + (i + 1) + ": " + e.getMessage());
                //             e.printStackTrace();
                //         }
                //     }

                //     ClientAgg.getRemoteDatasets(node,
                //             "/home/" + node + "/Documents/spark-mllib-grpc-dev/datasets/AARes.csv",
                //             "/home/pe01-vm05/Documents/spark-mllib-grpc-dev/received_files");

                // } finally {
                //     ClientAgg.shutdown();
                 //}
          //  }

            // File outputFolder = new File("/home/pe01-vm05/Documents/spark-mllib-grpc-dev/datasets/agg_results.csv");
            // if (outputFolder.exists()) {
            //     cleanCSV("/home/pe01-vm05/Documents/spark-mllib-grpc-dev/datasets/agg_results.csv", 2);
            // }

            // aggregateCSVFiles("/home/pe01-vm05/Documents/spark-mllib-grpc-dev/received_files",
            //         "/home/pe01-vm05/Documents/spark-mllib-grpc-dev/datasets/agg_results.csv");

    /////////////////////////////CLUSTRING of CLUSTERS//////////////////////////////
    /// 
    ///   
      for (String node : nodes) {
                ClientAgg ClientAgg = new ClientAgg(node, 50051);
            long start = System.currentTimeMillis();
            ClientAgg.applyAnalytics("/home/"+  node +"/Documents/spark-mllib-grpc-dev/datasets/clustring/bank_500.csv",
                    "/home/"+  node +"/Documents/spark-mllib-grpc-dev/outputDataset");
            ClientAgg.getRemoteDatasets(
                    "/home/"+  node +"/Documents/spark-mllib-grpc-dev/outputDataset/"+  node +"_kmeans_bank_500.csv",
                    "/home/ "+System.getProperty("user.name")+"/Documents/spark-mllib-grpc-dev/received_files");

            SparkConf conf = new SparkConf()
                    .setAppName("ClusterOfClusters")
                    .setMaster("local[*]"); // Use local mode for testing
            JavaSparkContext jsc = new JavaSparkContext(conf);

            // List of dataset file paths (CSV files)
                     List<String> receivedPaths = getCsvFiles("/home/"+System.getProperty("user.name")+"/Documents/spark-mllib-grpc-dev/received_files");

            // Output directory
            String outputDir = "/home/"+System.getProperty("user.name")+"/Documents/spark-mllib-grpc-dev/clusterComb/kmeans_bank_500_clusterOfclusters.csv";

            // Number of clusters and iterations
            int numClusters = 5;
            int numIterations = 20;

            // Create the clustering object
            ClusterAgg_V1 clustering = new ClusterAgg_V1(
                    jsc,
                    receivedPaths,
                    outputDir,
                    numClusters,
                    numIterations);

            // Run clustering
            clustering.runClustering();
            long end = System.currentTimeMillis();
            logExecutionTime(start, end, "/home/"+  node +"/grpc-java-examples-master/received_files/ismail_kmeans_bank_500.csv",
                    "/home/"+  node +"/grpc-java-examples-master/clusterComb/executionTime.csv");
            // Stop Spark
            jsc.close();
      }
    
        } else {

            ClientAgg ClientAgg = new ClientAgg("localhost", 50051);
            long start = System.currentTimeMillis();
            ClientAgg.applyAnalytics("/home/ismail/grpc-java-examples-master/clustring/bank_500.csv",
                    "/home/ismail/grpc-java-examples-master/outputDataset");
            ClientAgg.getRemoteDatasets(
                    "/home/ismail/grpc-java-examples-master/outputDataset/ismail_kmeans_bank_500.csv",
                    "/home/ismail/grpc-java-examples-master/received_files");
            // ClientAgg.getRemoteDatasets("/home/ismail/grpc-java-examples-master/outputDataset/executionTime.csv",
            //         "/home/ismail/grpc-java-examples-master/received_files");

            SparkConf conf = new SparkConf()
                    .setAppName("ClusterOfClusters")
                    .setMaster("local[*]"); // Use local mode for testing
            JavaSparkContext jsc = new JavaSparkContext(conf);

            // List of dataset file paths (CSV files

                     List<String> receivedPaths = getCsvFiles("/home/ismail/grpc-java-examples-master/received_files");

            // Output directory
            String outputDir = "/home/ismail/grpc-java-examples-master/clusterComb/ismail_kmeans_bank_500_clusterOfclusters.csv";

            // Number of clusters and iterations
            int numClusters = 5;
            int numIterations = 20;

            // Create the clustering object
            ClusterAgg_V1 clustering = new ClusterAgg_V1(
                    jsc,
                    receivedPaths,
                    outputDir,
                    numClusters,
                    numIterations);

            // Run clustering
            clustering.runClustering();
            long end = System.currentTimeMillis();
            logExecutionTime(start, end, "/home/ismail/grpc-java-examples-master/received_files/ismail_kmeans_bank_500.csv",
                    "/home/ismail/grpc-java-examples-master/clusterComb/executionTime.csv");
            // Stop Spark
            jsc.close();

            ////////////////////////////////////////////////////////////// AA
            ////////////////////////////////////////////////////////////// part//////////////////////////////////////////////////////////////////////////////////////
            ///
            ///
            ///
            // String quasiIdentifierFile =
            ////////////////////////////////////////////////////////////// "/home/ismail/grpc-java-examples-master/datasets/quasi_identifiers.dat";
            // List<List<String>> allQuasiIdentifiers =
            ////////////////////////////////////////////////////////////// readQuasiIdentifiersFromFile(quasiIdentifierFile);
            // String resultsFile =
            ////////////////////////////////////////////////////////////// "/home/ismail/grpc-java-examples-master/datasets/AARes.csv";

            // for (int i = 2; i < 11; i++) {
            // try {
            // System.out.println("Processing round " + (i ) + " with quasi-identifiers: "
            // + allQuasiIdentifiers.get(i));
            // String originalFile =
            // "/home/ismail/grpc-java-examples-master/datasets/banking_synthetic_v1.csv";
            // String anonymizedFile =
            // "/home/ismail/grpc-java-examples-master/datasets/anonymized_bank_A"+(i)+".csv";
            // List<String> quasiIdentifiers = allQuasiIdentifiers.get(i);

            // System.out.println("Processing round " + (i + 1) + " with quasi-identifiers:
            // " + quasiIdentifiers);

            // // --- Trigger the new service and get the score ---
            // ClientAgg.triggerAACalculation(originalFile, anonymizedFile,
            // quasiIdentifiers, resultsFile);

            // } catch (Exception e) {
            // System.out.println("Error processing round " + (i + 1) + ": " +
            // e.getMessage());
            // e.printStackTrace();
            // }
            // }

            // ClientAgg.getRemoteDatasets("","/home/ismail/grpc-java-examples-master/datasets/AARes.csv",
            // "/home/ismail/grpc-java-examples-master/received_files");

            // ClientAgg.aggregateCSVFiles("/home/ismail/grpc-java-examples-master/received_files",
            // "/home/ismail/grpc-java-examples-master/datasets/agg_results.csv");

            ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            ///
            ///
            ///

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
            ClientAgg.shutdown();
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

    public static void aggregateCSVFiles(String folderPath, String outputPath) throws IOException {
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

    public static void cleanCSV(String filePath, int expectedColumns) throws IOException {
        // Read the file and clean its content
        List<String> cleanedLines = new ArrayList<>();

        try (BufferedReader reader = Files.newBufferedReader(Paths.get(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim(); // Remove leading/trailing spaces

                // Skip empty lines
                if (line.isEmpty()) {
                    continue;
                }

                // Split the line into columns
                String[] columns = line.split(",");

                // Check if the line has the expected number of columns
                if (columns.length == expectedColumns) {
                    // Trim each column (field) to remove extra spaces
                    for (int i = 0; i < columns.length; i++) {
                        columns[i] = columns[i].trim();
                    }

                    // Reconstruct the line and add it to cleanedLines
                    String cleanedLine = String.join(",", columns);
                    cleanedLines.add(cleanedLine);
                } else {
                    // Skip lines that do not have the expected number of columns
                    System.out.println("Skipping invalid line: " + line);
                }
            }
        }

        // Now overwrite the original file with the cleaned content
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(filePath))) {
            for (String cleanedLine : cleanedLines) {
                writer.write(cleanedLine);
                writer.newLine();
            }
        }
    }

     public static void logExecutionTime(long startTime, long endTime, String datasetPath, String resultsPath) {
        long executionTime = endTime - startTime;
        int tuplesNumber = getDatasetRowCount(datasetPath);

        try {
            // Create results file with header if it doesn't exist
            if (!Files.exists(Paths.get(resultsPath))) {
                try (FileWriter writer = new FileWriter(resultsPath)) {
                    writer.append("tuplesNumber,executionTime\n");
                }
            }

            // Append new record to results CSV
            try (FileWriter writer = new FileWriter(resultsPath, true)) {
                writer.append(tuplesNumber + "," + executionTime + "\n");
            }

            System.out.println("Execution time logged successfully: " 
                                + tuplesNumber + " tuples, " 
                                + executionTime + " ms");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private static int getDatasetRowCount(String datasetPath) {
        int count = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(datasetPath))) {
            while (br.readLine() != null) {
                count++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        // subtract 1 for header
        return (count > 0) ? count - 1 : 0;
    }

    public static List<String> getCsvFiles(String dirPath) {
        List<String> csvFiles = new ArrayList<>();
        File directory = new File(dirPath);

        if (directory.exists() && directory.isDirectory()) {
            File[] files = directory.listFiles((dir, name) -> name.toLowerCase().endsWith(".csv"));
            if (files != null) {
                for (File file : files) {
                    csvFiles.add(file.getAbsolutePath());
                }
            }
        } else {
            System.err.println("Invalid directory: " + dirPath);
        }

        return csvFiles;
    }
}

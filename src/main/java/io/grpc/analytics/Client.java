package io.grpc.analytics;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.ByteString;

public class Client {
    private static final Logger logger = Logger.getLogger(Client.class.getName());
    private final ManagedChannel channel;
    private final ClustringAnalysisGrpc.ClustringAnalysisBlockingStub blockingStub;
    private final FrequentItemsGrpc.FrequentItemsBlockingStub blockingStubFP;
    private final RandomForestGrpc.RandomForestBlockingStub blockingStubRandomForest;
    private final LinearRegressionGrpc.LinearRegressionBlockingStub blockingStubLinearRegression;
    private final DatasetAccessGrpc.DatasetAccessBlockingStub blockingStubDatasetAccess;

    public Client(String host, int port) {
        channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext() // Note: For production, use proper authentication
                .build();
        blockingStub = ClustringAnalysisGrpc.newBlockingStub(channel);
        blockingStubFP = FrequentItemsGrpc.newBlockingStub(channel);
        this.blockingStubRandomForest = RandomForestGrpc.newBlockingStub(channel);
        this.blockingStubLinearRegression = LinearRegressionGrpc.newBlockingStub(channel);
        this.blockingStubDatasetAccess = DatasetAccessGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public void applyAnalytics(String datasetPath, String outputPath) {
        RequestClustringKmeans request = RequestClustringKmeans.newBuilder()
                .setDatasetPath(datasetPath)
                .setOutputPath(outputPath)
                .build();
        try {
            ResponseClustringKmeans response = blockingStub.clustringKmeansServer(request);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
        }
    }

    public void applyFpGrowth(String datasetPath, String outputPath) {
        String mainPath = readSettings("DATASET_PATH");
        mainPath = Paths.get(mainPath).toAbsolutePath().toString();
        RequestFrequentItems request = RequestFrequentItems.newBuilder()
                .setDatasetPath(datasetPath)
                .setOutputPath(outputPath)
                .build();
        try {
            ResponseFrequentItems response = blockingStubFP.ftGrowth(request);

        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
        }
    }

    public void applyRandomForest(String datasetPath, String outputPath) {
        RequestRandomForest request = RequestRandomForest.newBuilder()
                .setDatasetPath(datasetPath)
                .setOutputPath(outputPath)
                .build();
        try {
            ResponseRandomForest response = blockingStubRandomForest.randomForestAnalytics(request);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
        }
    }

    public void applyLinearRegression(String datasetPath, String outputPath) {
        RequestLinearRegression request = RequestLinearRegression.newBuilder()
                .setDatasetPath(datasetPath)
                .setOutputPath(outputPath)
                .build();
        try {
            // Ensure received_files directory exists

            RequestLinearRegression response = blockingStubLinearRegression.linearRegressionAnalytics(request);

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
        String MODE = "PRODUCTION";
        // String MODE = "DEVELOPMENT";
        if (MODE == "PRODUCTION") {
            // List<String> nodes = Arrays.asList("pe01-vm04", "pe01-vm05", "pe01-vm06",
            // "pe02-vm04", "pe02-vm05", "pe02-vm06");
            List<String> nodes = Arrays.asList("pe01-vm05", "pe01-vm06");

            for (String node : nodes) {
                Client client = new Client(node, 50051);

                try {
                    client.applyLinearRegression("/home/" + node + "/Documents/datasets",
                            "/home/" + node + "/Documents/output/" + node + "_LinearRegression");
                    client.getRemoteDatasets("/home/" + node + "/Documents/output/" + node + "_LinearRegression",
                            "/home/pe01-vm03/Documents/agg");
                } finally {
                    client.shutdown();
                }
            }
        } else {
            Client client = new Client("localhost", 50051);
            // client.applyAnalytics("/home/ismail/grpc-java-examples-master/datasets",
            // "/home/ismail/grpc-java-examples-master/outputDataset");
            // client.applyFpGrowth("/home/ismail/grpc-java-examples-master/datasets",
            // "outputPath_FpGrowthXXX"); //// WE ADD TO THE PATH THE NODE NAME FROM THE FOR
            // LOOP
            // client.applyRandomForest("/home/ismail/grpc-java-examples-master/datasets",
            // "outputPath_RandomForestXXX");
            // client.applyLinearRegression("/home/ismail/grpc-java-examples-master/datasets",
            // "outputPath_LinearRegressionXXX");

            client.applyAnalytics("/home/ismail/grpc-java-examples-master/datasets",
                    "/home/ismail/grpc-java-examples-master/outputDataset");

            AnonymizationAccuracyGrpc.AnonymizationAccuracyBlockingStub stub = AnonymizationAccuracyGrpc
                    .newBlockingStub(client.channel);

            List<String> quasiIdentifiers = Arrays.asList("age", "workclass", "education", "occupation");

            RequestBatchAnonymizationAccuracy request = RequestBatchAnonymizationAccuracy.newBuilder()
                    .setDatasetBaseName("adult_data")
                    .setNumDatasets(5) // The number of anonymized datasets (e.g., adult_data_A1.csv, ...,
                                       // adult_data_A5.csv)
                    .setOriginalDatasetBasePath("path/to/your/original") // Path to the directory of the original
                                                                         // dataset
                    .setAnonymizedDatasetBasePath("path/to/your/anonymized") // Path to the directory of anonymized
                                                                             // datasets
                    .addAllQuasiIdentifierNames(quasiIdentifiers)
                    .setOutputPath("path/to/your/output") // Where to save the results CSV
                    .build();

            // 5. Make the remote call and get the response.
            ResponseBatchAnonymizationAccuracy response = stub.calculateBatchECS(request);

            // 6. Print the results from the response.
            System.out.println("✅ Batch ECS Calculation Status: " + response.getStatus());
            if ("SUCCESS".equals(response.getStatus())) {
                System.out.println("Total datasets to process: " + response.getTotalDatasets());
                System.out.println("Successfully processed: " + response.getSuccessfulDatasets());
                System.out.println("Failed: " + response.getFailedDatasets());
                System.out.printf("Mean ECS Score: %.4f%n", response.getMeanEcsScore());
                System.out.printf("Std Dev ECS Score: %.4f%n", response.getStdEcsScore());
                System.out.printf("Min ECS Score: %.4f%n", response.getMinEcsScore());
                System.out.printf("Max ECS Score: %.4f%n", response.getMaxEcsScore());
                System.out.println("Results saved to the specified output path.");
            } else {
                System.out.println("❌ Error: " + response.getErrorMessage());
            }

            client.getRemoteDatasets("/home/ismail/grpc-java-examples-master/outputPath_LinearRegressionXXX",
                    "/home/ismail/grpc-java-examples-master/received_files");
            client.shutdown();
        }

    }

    /**********************************************************************
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

}

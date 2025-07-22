package io.grpc.analytics;

import io.grpc.Server;
import io.grpc.Status;

import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.ivy.plugins.repository.ssh.Scp.FileInfo;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;


public class GrpcServer {
    private static final Logger logger = Logger.getLogger(GrpcServer.class.getName());
    private static final Set<String> ALLOWED_EXTENSIONS = new HashSet<>(Arrays.asList(
            "csv", "bat", "txt"));
    /* The port on which the server should run */
    private final int port;
    private final Server server;

    public GrpcServer(int port) throws IOException {
        this.port = port;
        server = NettyServerBuilder.forPort(port)
                .addService(new ClustringAnalysis())
                .addService(new FrequentItemsImpl())
                .addService(new DatasetAccessImpl())
                .addService(new RandomForestImpl())
                .addService(new LinearRegressionImpl())
                .addService(new AnonymityServiceImpl())
                .build()
                .start();

        logger.info("Server started, listening on " + port);
    }

    public GrpcServer() throws IOException {
        this(50051);
    }

    public void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon
     * threads.
     */
    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    private static class ClustringAnalysis extends ClustringAnalysisGrpc.ClustringAnalysisImplBase {
        @Override
        public void clustringKmeansServer(RequestClustringKmeans req,
                StreamObserver<ResponseClustringKmeans> responseObserver) {
            SparkConf conf = new SparkConf()
                    .setAppName("KMeans Clustering Example")
                    .setMaster("local[*]");
            JavaSparkContext jsc = new JavaSparkContext(conf);

            try {
                // Define parameters
                String datasetsDirectory = req.getDatasetPath(); // Directory containing all datasets
                String outputDir = req.getOutputPath();
                int numClusters = 5;
                int numIterations = 20;

                // Create output directory if it doesn't exist
                File outputDirFile = new File(outputDir);
                if (!outputDirFile.exists()) {
                    outputDirFile.mkdirs();
                }

                // Find all CSV files in the datasets directory
                List<Path> datasetPaths = findCSVFiles(datasetsDirectory);

                if (datasetPaths.isEmpty()) {
                    System.out.println("No CSV datasets found in directory: " + datasetsDirectory);
                    return;
                }

                System.out.println("Found " + datasetPaths.size() + " CSV datasets to process");
                List<KMeansClusteringAnalytics.ClusteringResult> allResults = new ArrayList<>();
                for (Path datasetPath : datasetPaths) {
                    String fullPath = datasetPath.toString();
                    String fileName = datasetPath.getFileName().toString();

                    // Extract dataset name from filename (remove .csv extension)
                    String datasetName = fileName;
                    if (fileName.toLowerCase().endsWith(".csv")) {
                        datasetName = fileName.substring(0, fileName.length() - 4);
                    }

                    System.out.println("***********========================================");
                    System.out.println("Processing dataset: " + datasetName);

                    try {
                        // Create and run the K-means clustering analysis
                        KMeansClusteringAnalytics analytics = new KMeansClusteringAnalytics(
                                jsc, fullPath, outputDir, datasetName, numClusters, numIterations);

                        KMeansClusteringAnalytics.ClusteringResult result = analytics.runClustering();
                        allResults.add(result);
                    } catch (Exception e) {
                        System.err.println("Error processing dataset " + datasetName + ": " + e.getMessage());
                        e.printStackTrace();
                        // Continue with the next dataset
                    }
                }

                System.out.println("Total datasets processed: " + allResults.size());

            } catch (Exception e) {
                System.err.println("Error in batch processing: " + e.getMessage());
                e.printStackTrace();
            } finally {
                jsc.stop();
            }

        }
    }

    private static class FrequentItemsImpl extends FrequentItemsGrpc.FrequentItemsImplBase {
        @Override
        public void ftGrowth(RequestFrequentItems req, StreamObserver<ResponseFrequentItems> responseObserver) {

            try {
                SparkConf conf = new SparkConf().setAppName("ftGrowth").setMaster("local");
                String datasetsDirectory = req.getDatasetPath(); // Directory containing all datasets
                String outputDir = req.getOutputPath();
                double minSupport = 0.05;
                int minConfidence = 20;

                // Create output directory if it doesn't exist
                File outputDirFile = new File(outputDir);
                if (!outputDirFile.exists()) {
                    outputDirFile.mkdirs();
                }

                // Find all CSV files in the datasets directory
                List<Path> datasetPaths = findCSVFiles(datasetsDirectory);

                System.out.println("Found " + datasetPaths.size() + " CSV datasets to process");

                // Process each dataset file
                List<String> processedDatasets = new ArrayList<>();
                List<String> failedDatasets = new ArrayList<>();

                for (Path datasetPath : datasetPaths) {
                    String fullPath = datasetPath.toString();
                    String fileName = datasetPath.getFileName().toString();

                    // Extract dataset name from filename (remove .csv extension)
                    String datasetName = fileName;
                    if (fileName.toLowerCase().endsWith(".csv")) {
                        datasetName = fileName.substring(0, fileName.length() - 4);
                    }

                    System.out.println("\nProcessing dataset: " + datasetName);

                    try {
                        // Create output path specific to this dataset
                        String datasetOutputPath = outputDir + "/" + System.getProperty("user.name") + "_fpgrowth_"
                                + datasetName;

                        // Run FPgrowth analysis for this dataset
                        FPgrowth ftgrowth = new FPgrowth(conf, fullPath, minSupport, minConfidence, datasetOutputPath);
                        ftgrowth.analyze();

                        processedDatasets.add(datasetName);

                    } catch (Exception e) {
                        System.err.println("Error processing dataset " + datasetName + ": " + e.getMessage());
                        failedDatasets.add(datasetName);
                        // Continue with the next dataset
                    }
                }

            } catch (Exception e) {
                responseObserver.onError(Status.INTERNAL
                        .withDescription("Error in batch processing: " + e.getMessage())
                        .asRuntimeException());
            }
        }
    }

    private static class DatasetAccessImpl extends DatasetAccessGrpc.DatasetAccessImplBase {
        @Override
        public void remoteDataset(RequestDatasetAccess req, StreamObserver<ResponseDatasetAccess> responseObserver) {
            String path = req.getFolderPath();

            try {
                File source = new File(path);

                // Validate path exists
                if (!source.exists()) {
                    throw new FileNotFoundException("Path not found: " + source.getAbsolutePath());
                }

                // Create builder for the response
                ResponseDatasetAccess.Builder responseBuilder = ResponseDatasetAccess.newBuilder();

                if (source.isFile()) {
                    // Process single file if it has an allowed extension
                    if (hasAllowedExtension(source.getName())) {
                        processFile(source, "", responseBuilder);
                    } else {
                        System.out.println("Skipped file with unsupported extension: " + source.getName());
                    }
                } else if (source.isDirectory()) {
                    // Process directory and its contents recursively
                    processDirectory(source, "", responseBuilder);
                }

                // Send the response with all files
                ResponseDatasetAccess response = responseBuilder.build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();

            } catch (Exception e) {
                // Handle any errors during processing
                System.err.println("Error processing request: " + e.getMessage());
                e.printStackTrace();

                // Send error response
                responseObserver.onError(Status.INTERNAL
                        .withDescription("Error processing path contents: " + e.getMessage())
                        .asRuntimeException());
            }
        }
    }

    private static class RandomForestImpl extends RandomForestGrpc.RandomForestImplBase {
        @Override
        public void randomForestAnalytics(RequestRandomForest req,
                StreamObserver<ResponseRandomForest> responseObserver) {
            try {
                SparkSession spark = SparkSession.builder()
                        .appName("BatchRandomForestAnalysis")
                        .master("local[*]")
                        .getOrCreate();

                String datasetsDirectory = req.getDatasetPath(); // Directory containing all datasets
                String outputDir = req.getOutputPath();

                // Create output directory if it doesn't exist
                File outputDirFile = new File(outputDir);
                if (!outputDirFile.exists()) {
                    outputDirFile.mkdirs();
                }

                // Find all CSV files in the datasets directory
                List<Path> datasetPaths = findCSVFiles(datasetsDirectory);
                System.out.println("Found " + datasetPaths.size() + " CSV datasets to process");

                // Process each dataset file
                List<String> processedDatasets = new ArrayList<>();
                List<String> failedDatasets = new ArrayList<>();

                for (Path datasetPath : datasetPaths) {
                    String fullPath = datasetPath.toString();
                    String fileName = datasetPath.getFileName().toString();

                    // Extract dataset name from filename (remove .csv extension)
                    String datasetName = fileName;
                    if (fileName.toLowerCase().endsWith(".csv")) {
                        datasetName = fileName.substring(0, fileName.length() - 4);
                    }

                    System.out.println("\nProcessing dataset: " + datasetName);

                    try {
                        String datasetOutputPath = outputDir + "/" + System.getProperty("user.name") + "_rf_results_"
                                + datasetName;
                        RandomForestAnalytics analytics = new RandomForestAnalytics(spark, fullPath, datasetOutputPath);
                        analytics.runAnalysis();
                        processedDatasets.add(datasetName);
                    } catch (Exception e) {
                        System.err.println("Error processing dataset " + datasetName + ": " + e.getMessage());
                        failedDatasets.add(datasetName);
                    }
                }
                spark.stop();

            } catch (Exception e) {
                responseObserver.onError(Status.INTERNAL
                        .withDescription("Error in batch processing: " + e.getMessage())
                        .asRuntimeException());
            }
        }
    }

    private static class LinearRegressionImpl extends LinearRegressionGrpc.LinearRegressionImplBase {
        @Override
        public void linearRegressionAnalytics(RequestLinearRegression req,
                StreamObserver<RequestLinearRegression> responseObserver) {
            SparkSession spark = SparkSession.builder()
                    .appName("Batch Linear Regression Analysis")
                    .master("local[*]")
                    .getOrCreate();

            try {
                String datasetsDirectory = req.getDatasetPath(); // Directory containing all datasets
                String outputDir = req.getOutputPath();

                // Create output directory if it doesn't exist
                File outputDirFile = new File(outputDir);
                if (!outputDirFile.exists()) {
                    outputDirFile.mkdirs();
                }
                List<Path> datasetPaths = findCSVFiles(datasetsDirectory);
                System.out.println("Found " + datasetPaths.size() + " CSV datasets to process");
                List<String> processedDatasets = new ArrayList<>();
                List<String> failedDatasets = new ArrayList<>();

                for (Path datasetPath : datasetPaths) {
                    String fullPath = datasetPath.toString();
                    String fileName = datasetPath.getFileName().toString();

                    // Extract dataset name from filename (remove .csv extension)
                    String datasetName = fileName;
                    if (fileName.toLowerCase().endsWith(".csv")) {
                        datasetName = fileName.substring(0, fileName.length() - 4);
                    }

                    System.out.println("\nProcessing dataset: " + datasetName);

                    try {
                        // Create output path specific to this dataset
                        String datasetOutputPath = outputDir + "/" + datasetName + "_linreg_results";

                        // Run Linear Regression analysis for this dataset
                        LinearRegressionAnalytics analytics = new LinearRegressionAnalytics(spark, fullPath,
                                datasetOutputPath);
                        LinearRegressionAnalytics.LinearRegressionResult result = analytics.runAnalysis();

                        // Print the results
                        System.out.println("Results for dataset " + datasetName + ":");
                        System.out.println(result);

                        processedDatasets.add(datasetName);

                    } catch (Exception e) {
                        System.err.println("Error processing dataset " + datasetName + ": " + e.getMessage());
                        failedDatasets.add(datasetName);
                        // Continue with the next dataset
                    }
                }

                // Build response with summary
                StringBuilder resultMessage = new StringBuilder();
                resultMessage.append("Batch Linear Regression processing complete.\n");
                resultMessage.append("Total datasets: ").append(datasetPaths.size()).append("\n");
                resultMessage.append("Successfully processed: ").append(processedDatasets.size()).append("\n");
                resultMessage.append("Failed: ").append(failedDatasets.size()).append("\n\n");

                if (!failedDatasets.isEmpty()) {
                    resultMessage.append("Failed datasets: ").append(String.join(", ", failedDatasets));
                }

                responseObserver.onNext(RequestLinearRegression.newBuilder()
                        .build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                System.err.println("Error running batch analytics: " + e.getMessage());
                e.printStackTrace();
                responseObserver.onError(Status.INTERNAL
                        .withDescription("Error processing datasets: " + e.getMessage())
                        .asRuntimeException());
            } finally {
                // Stop Spark session after batch processing
                spark.stop();
            }
        }
    }

private static class AnonymityServiceImpl extends AnonymityServiceGrpc.AnonymityServiceImplBase {
        @Override
        public void calculateAA(AARequest req, StreamObserver<AAResponse> responseObserver) {
            logger.info("Received Anonymization Accuracy request.");
            AAResponse.Builder responseBuilder = AAResponse.newBuilder();
            try {
                // 1. Instantiate the analytics class with paths from the request
                AnonymizationAccuracyAnalytics analytics = new AnonymizationAccuracyAnalytics(
                        req.getOriginalCsvPath(),
                        req.getAnonymizedCsvPath(),
                        req.getQuasiIdentifierColumnsList(),
                        req.getOutputResultsPath()
                );

                // 2. Run the calculation
                double score = analytics.runCalculation();

                // 3. Build and send a SUCCESS response
                responseBuilder.setStatus(AAResponse.Status.SUCCESS)
                               .setAaScore(score)
                               .setMessage("Successfully calculated AA. Score: " + score);
                // logger.info("AA calculation successful. Score: " + score);

            } catch (Exception e) {
                // 4. Build and send an ERROR response
                logger.severe("Error calculating AA: " + e.getMessage());
                responseBuilder.setStatus(AAResponse.Status.ERROR)
                               .setMessage("Failed to calculate AA: " + e.getMessage());
            }
            
            // 5. Send the response to the client
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        }
    }

    
    public static void main(String[] args) throws IOException, InterruptedException {
        // Create server instance
        final GrpcServer server = new GrpcServer();

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("*** shutting down gRPC server since JVM is shutting down");
            try {
                server.stop();
            } catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
            System.err.println("*** server shut down");
        }));

        // Block and wait for shutdown
        
        server.blockUntilShutdown();
    }

    /************************** UTILS */
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
            e.printStackTrace();
        }
        return null;
    }

    public static void writeExecutionTimeToCSV(String csvFile, double executionTimeInSeconds) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(csvFile, true))) {
            writer.write("Excution Time" + "," + executionTimeInSeconds + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static List<Path> findCSVFiles(String directory) throws Exception {
        try (Stream<Path> paths = Files.walk(Paths.get(directory))) {
            return paths
                    .filter(Files::isRegularFile)
                    .filter(path -> path.toString().toLowerCase().endsWith(".csv"))
                    .collect(Collectors.toList());
        }
    }

    private static boolean hasAllowedExtension(String fileName) {
        int dotIndex = fileName.lastIndexOf('.');
        if (dotIndex > 0 && dotIndex < fileName.length() - 1) {
            String extension = fileName.substring(dotIndex + 1).toLowerCase();
            return ALLOWED_EXTENSIONS.contains(extension);
        }
        return false;
    }

    /**
     * Recursively processes a directory and all its contents
     */
    private static void processDirectory(File directory, String relativePath,
            ResponseDatasetAccess.Builder responseBuilder) throws IOException {
        File[] files = directory.listFiles();
        if (files == null || files.length == 0) {
            System.out.println("No files found in directory: " + directory.getAbsolutePath());
            return;
        }

        for (File file : files) {
            String currentRelativePath = relativePath.isEmpty() ? file.getName()
                    : relativePath + File.separator + file.getName();

            if (file.isFile()) {
                if (hasAllowedExtension(file.getName())) {
                    processFile(file, relativePath, responseBuilder);
                } else {
                    System.out.println("Skipped file with unsupported extension: " + file.getName());
                }
            } else if (file.isDirectory()) {
                // Process subdirectory recursively
                processDirectory(file, currentRelativePath, responseBuilder);
            }
        }
    }

    /**
     * Processes a single file and adds it to the response builder
     */
    private static void processFile(File file, String relativePath, ResponseDatasetAccess.Builder responseBuilder)
            throws IOException {
        // Read file content
        ByteString fileContent = ByteString.copyFrom(Files.readAllBytes(file.toPath()));

        // Create the file path for the response
        String filePath;
        if (relativePath.isEmpty()) {
            filePath = file.getName();
        } else {
            filePath = relativePath + File.separator + file.getName();
        }

        // Add file to response
        FileData fileData = FileData.newBuilder()
                .setFileName(filePath) // Include relative path to preserve directory structure
                .setContent(fileContent)
                .build();

        responseBuilder.addFiles(fileData);
        System.out.println("Added file to response: " + filePath);
    }

}
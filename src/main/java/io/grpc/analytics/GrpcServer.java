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
import java.nio.file.Paths;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.SparkSession;

import com.google.protobuf.ByteString;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Server that manages startup/shutdown of a {@code Greeter} server.
 */
public class GrpcServer {
    private static final Logger logger = Logger.getLogger(GrpcServer.class.getName());

    /* The port on which the server should run */
    private final int port;
    private final Server server;

    public GrpcServer(int port) throws IOException {
        this.port = port;
        server = NettyServerBuilder.forPort(port)
                .addService(new GreeterImpl())
                .addService(new FrequentItemsImpl())
                .addService(new DatasetAccessImpl())
                .addService(new RandomForestImpl())
                .addService(new LinearRegressionImpl())
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

    private static class GreeterImpl extends GreeterGrpc.GreeterImplBase {
        @Override
        public void clustringKmeansServer(Request req, StreamObserver<Response> responseObserver) {
            // try {
            //     // Log start of processing
            //     System.out.println(
            //             "***************************** RUN CODE HERE *****************************************************");
            //     long startTime = System.nanoTime();
            //     // Perform clustering
            //     clustringKmeans(req.getDatasetPath(), req.getDatasetName(), req.getAlgorithm());

            //     // // Calculate execution time
            //     // long endTime = System.nanoTime();
            //     // double executionTimeInSeconds = (endTime - startTime) / 1_000_000_000.0;
            //     // writeExecutionTimeToCSV("executionTime", executionTimeInSeconds);

            //     // Prepare file to send
            //     File fileToSend = new File(req.getAlgorithm() + "_" + req.getDatasetName() + ".csv");

            //     // Validate file exists
            //     if (!fileToSend.exists()) {
            //         throw new FileNotFoundException("Output file not found: " + fileToSend.getAbsolutePath());
            //     }

            //     // Read entire file content
            //     byte[] fileContent = java.nio.file.Files.readAllBytes(fileToSend.toPath());

            //     // Create a single response with full file content
            //     Response response = Response.newBuilder()
            //             .setFileName(fileToSend.getName())
            //             .setFileContent(com.google.protobuf.ByteString.copyFrom(fileContent))
            //             .setNodeName(System.getProperty("user.name"))
            //             .build();

            //     // Send the response
            //     responseObserver.onNext(response);
            //     responseObserver.onCompleted();

            // } catch (Exception e) {
            //     // Handle any errors during processing
            //     System.err.println("Error processing request: " + e.getMessage());
            //     e.printStackTrace();

            //     // Send error response
            //     responseObserver.onError(Status.INTERNAL
            //             .withDescription("Error processing file: " + e.getMessage())
            //             .asRuntimeException());
            // }

            SparkConf conf = new SparkConf()
            .setAppName("KMeans Clustering Example")
            .setMaster("local[*]");
    JavaSparkContext jsc = new JavaSparkContext(conf);
            
    try {
        // Define parameters should be passed from the client as request parameters
        String datasetPath = "/home/ismail/grpc-java-examples-master/insurance_v1.csv";
        String outputDir = "complete_cluster_assignments";
        String datasetName = "insurance_v1";
        int numClusters = 5;
        int numIterations = 20;
        
        // Create and run the K-means clustering analysis
        KMeansClusteringAnalytics analytics = new KMeansClusteringAnalytics(
            jsc, datasetPath, outputDir, datasetName, numClusters, numIterations);
            
        KMeansClusteringAnalytics.ClusteringResult result = analytics.runClustering();
        
        // Print the results
        System.out.println("Clustering completed with results:");
        System.out.println(result);
        System.out.println("Results saved to: " + result.getOutputFilePath());
        
    } catch (Exception e) {
        System.err.println("Error running clustering: " + e.getMessage());
        e.printStackTrace();
    } finally {
        jsc.stop();
    }
        }
    }

    private static class FrequentItemsImpl extends FrequentItemsGrpc.FrequentItemsImplBase {
        @Override
        public void ftGrowth(RequestFrequentItems req, StreamObserver<ResponseFrequentItems> responseObserver) {
            System.out.println(req.getDatasetPath());
            try {
                SparkConf conf = new SparkConf().setAppName("ftGrowth").setMaster("local");
                String datasetPath = req.getDatasetPath();
                String datasetName = req.getDatasetName();
                String outputPath = req.getOutputPath();
                FPgrowth ftgrowth = new FPgrowth(conf, datasetPath, 0.05, 20,
                        outputPath, datasetName);
                ftgrowth.analyze();

                File fileToSend = new File(outputPath + "/" + System.getProperty("user.name") + "_"
                        + datasetName.split("\\.")[0] + "_FP_Growth.dat");
                byte[] fileContent = java.nio.file.Files.readAllBytes(fileToSend.toPath());

                ResponseFrequentItems response = ResponseFrequentItems.newBuilder()
                        .setFileName(
                                System.getProperty("user.name") + "_" + datasetName.split("\\.")[0] + "_FP_Growth.dat")
                        .setFileContent(com.google.protobuf.ByteString.copyFrom(fileContent))
                        .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } catch (Exception e) {
                responseObserver.onError(Status.INTERNAL
                        .withDescription("Error processing file: " + e.getMessage())
                        .asRuntimeException());
            }
        }
    }

    private static class DatasetAccessImpl extends DatasetAccessGrpc.DatasetAccessImplBase {
        @Override
        public void remoteDataset(RequestDatasetAccess req, StreamObserver<ResponseDatasetAccess> responseObserver) {
            String datasetName = req.getDatasetName(),
                    datasetPath = req.getDatasetPath();

            try {
                File sourceFolder = new File(datasetPath + "/" + datasetName);

                // Validate file/folder exists
                if (!sourceFolder.exists()) {
                    throw new FileNotFoundException("Dataset not found: " + sourceFolder.getAbsolutePath());
                }

                ByteString contentToSend;

                if (sourceFolder.isDirectory()) {
                    // If it's a directory, zip the contents
                    contentToSend = zipFolder(sourceFolder);
                    System.out.println("Zipped folder " + sourceFolder.getName() + " for transmission");
                } else {
                    // If it's a single file, just read the bytes
                    contentToSend = ByteString.copyFrom(Files.readAllBytes(sourceFolder.toPath()));
                    System.out.println("Read file " + sourceFolder.getName() + " for transmission");
                }

                // Create response with the content (either zipped folder or single file)
                ResponseDatasetAccess response = ResponseDatasetAccess.newBuilder()
                        .setFileContent(contentToSend)
                        .setIsZippedFolder(sourceFolder.isDirectory())
                        .build();

                // Send the response
                responseObserver.onNext(response);
                responseObserver.onCompleted();

                System.out.println("Successfully sent " + sourceFolder.getName());

            } catch (Exception e) {
                // Handle any errors during processing
                System.err.println("Error processing request: " + e.getMessage());
                e.printStackTrace();

                // Send error response
                responseObserver.onError(Status.INTERNAL
                        .withDescription("Error processing dataset: " + e.getMessage())
                        .asRuntimeException());
            }
        }

    }

    private static class RandomForestImpl extends RandomForestGrpc.RandomForestImplBase {
        @Override
        public void randomForestAnalytics(RequestRandomForest req,
                StreamObserver<ResponseRandomForest> responseObserver) {
            SparkSession spark = SparkSession.builder()
                    .appName("RandomForestExample")
                    .master("local[*]")
                    .getOrCreate();
            RandomForestAnalytics analytics = new RandomForestAnalytics(spark, req.getDatasetPath(),
                    req.getOutputPath());
            analytics.runAnalysis();
            spark.stop();
        }
    }

    private static class LinearRegressionImpl extends LinearRegressionGrpc.LinearRegressionImplBase {
        @Override
        public void linearRegressionAnalytics(RequestLinearRegression req,
                StreamObserver<RequestLinearRegression> responseObserver) {
            SparkSession spark = SparkSession.builder()
                    .appName("Linear Regression Example")
                    .master("local[*]")
                    .getOrCreate();
            try {
                // Define paths
                String datasetPath =  req.getDatasetPath();
                String outputDir =   req.getOutputPath();

                // Create and run the linear regression analysis
                LinearRegressionAnalytics analytics = new LinearRegressionAnalytics(spark, datasetPath, outputDir);
                LinearRegressionAnalytics.LinearRegressionResult result = analytics.runAnalysis();

                // Print the results
                System.out.println("Analysis completed with results:");
                System.out.println(result);

            } catch (Exception e) {
                System.err.println("Error running analytics: " + e.getMessage());
                e.printStackTrace();
            } finally {
                spark.stop();
            }
        }
    }

    /**
     * Main launches the server from the command line.
     */
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

    /****************************************************************************
     * k means
     */
    private static void clustringKmeans(String datasetPath, String datasetName, String algorithm) {
        // Create Spark configuration and context
      
    }

    private static ByteString zipFolder(File folderToZip) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ZipOutputStream zos = new ZipOutputStream(baos);

        // Get folder path for creating relative paths in zip
        String folderPath = folderToZip.getAbsolutePath();

        System.out.println("Creating zip from folder: " + folderPath);

        // Recursively add folder contents to zip
        addFolderToZip(folderToZip, folderToZip.getName(), zos);

        // Close the zip stream
        zos.close();

        // Convert to ByteString
        return ByteString.copyFrom(baos.toByteArray());
    }

    private static void addFolderToZip(File file, String entryPath, ZipOutputStream zos) throws Exception {
        if (file.isDirectory()) {
            // For directories, recursively process all contents
            File[] files = file.listFiles();

            // First, add this directory entry
            zos.putNextEntry(new ZipEntry(entryPath + "/"));
            zos.closeEntry();

            if (files != null) {
                for (File childFile : files) {
                    // Recursive call with updated entry path
                    addFolderToZip(childFile, entryPath + "/" + childFile.getName(), zos);
                }
            }
        } else {
            // For files, add file content to zip
            FileInputStream fis = new FileInputStream(file);

            // Create a new entry in the zip
            ZipEntry zipEntry = new ZipEntry(entryPath);
            zos.putNextEntry(zipEntry);

            // Write file content to zip
            byte[] buffer = new byte[1024];
            int length;
            while ((length = fis.read(buffer)) > 0) {
                zos.write(buffer, 0, length);
            }

            // Close resources
            zos.closeEntry();
            fis.close();
        }
    }
}
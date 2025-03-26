package io.grpc.clustringkmeans;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A simple client that requests a file from the {@link GrpcServer}.
 */
public class Client {
    private static final Logger logger = Logger.getLogger(Client.class.getName());

    private final ManagedChannel channel;
    private final GreeterGrpc.GreeterBlockingStub blockingStub;

    /** Construct client connecting to server at {@code host:port}. */
    public Client(String host, int port) {
        channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext() // Note: For production, use proper authentication
                .build();
        blockingStub = GreeterGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public void applyAnalytics(String datasetPath, String datasetName, String algorithm ) {
        logger.info("Requesting file for dataset: " + datasetName + " ...");
        Request request = Request.newBuilder()
        .setDatasetPath(datasetPath)
        .setDatasetName(datasetName)
        .setAlgorithm(algorithm)
        .build();

        try {
            // Ensure received_files directory exists
            File outputFolder = new File("received_files");
            if (!outputFolder.exists()) {
                outputFolder.mkdirs();
            }

            // Get the response from the server
            Response response = blockingStub.clustringKmeansServer(request);

            // Validate response
            if (response == null || response.getFileContent().isEmpty()) {
                logger.severe("No file content received from server");
                return;
            }

            // Sanitize filename
            String sanitizedFileName = response.getNodeName() + "_" + response.getFileName();
            System.out.println(response.getFileName() + response.getNodeName().toString());
            // Create output file
            File outputFile = new File(outputFolder, sanitizedFileName);

            // Write file content
            try (FileOutputStream fos = new FileOutputStream(outputFile)) {
                response.getFileContent().writeTo(fos);
                logger.info("File saved successfully: " + outputFile.getAbsolutePath());
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Error writing file: " + e.getMessage(), e);
            }

        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
        }
    }

    public static String readSettings(String param) {
        String filePath = "src/main/java/io/grpc/clustringkmeans/resources/settings.dat";
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

    public static void main(String[] args) throws Exception {
        Client client = new Client("localhost", 50051);
        try {
            String dataset_path = args.length > 0 ? args[0] : readSettings("DATASET_PATH");
            client.applyAnalytics(dataset_path,"insurance", "kmeans");
        } finally {
            client.shutdown();
        }
    }
}
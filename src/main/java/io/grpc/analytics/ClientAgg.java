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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.omg.CORBA.Any;

public class ClientAgg {

    private static final Logger logger = Logger.getLogger(Client.class.getName());

    private final ManagedChannel channel;
    private final DatasetAccessGrpc.DatasetAccessBlockingStub blockingStubFP;

    /** Construct client connecting to server at {@code host:port}. */
    public ClientAgg(String host, int port) {
        channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext() // Note: For production, use proper authentication
                .build();
        blockingStubFP = DatasetAccessGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public void getRemoteDatasets(String datasetName, String datasetPath) {
        RequestDatasetAccess request = RequestDatasetAccess.newBuilder()
        .setDatasetPath(datasetPath)
        .setDatasetName(datasetName)
        .setOutputPath("/home/ismail/grpc-java-examples-master/received_files")
        .build();

        try {
            // Ensure received_files directory exists
            File outputFolder = new File("received_files");
            if (!outputFolder.exists()) {
                outputFolder.mkdirs();
            }

            // Get the response from the server
            ResponseDatasetAccess response = blockingStubFP.remoteDataset(request);

            // Validate response
            if (response == null || response.getFileContent().isEmpty()) {
                logger.severe("No file content received from server");
                return;
            }

            // Sanitize filename
            // String sanitizedFileName = response.getNodeName() + "_" + response.getFileName();
            // System.out.println(response.getFileName() + response.getNodeName().toString());
            // Create output file
            File outputFile = new File(outputFolder, datasetName);

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

    public static void main(String[] args) throws Exception {

        String MODE = "dev"; // dev or prod

        if (MODE == "prod") {

            // List<String> nodes = Arrays.asList("pe01-vm04", "pe01-vm05", "pe01-vm06",
            // "pe02-vm04", "pe02-vm05", "pe02-vm06");
            List<String> nodes = Arrays.asList("");
            // List<String> nodes = Arrays.asList("pe01-vm06");

            // List of algorithms to run
            // List<String> algorithms = Arrays.asList("kanonymity", "ldiversity",
            // "tcloseness");
            List<String> algorithms = Arrays.asList("kanonymity", "ldiversity", "tcloseness");
            for (String node : nodes) {
                Client client = new Client(node, 50051);

                try {
                    String dataset_path = args.length > 0 ? args[0] : readSettings("DATASET_PATH");
                    // client.applyAnalytics(dataset_path,"insurance", "kmeans");

                    // Determine the dataset prefix based on the node (pe01 for banking, pe02 for
                    // insurance)
                    String datasetPrefix = node.startsWith("pe02") ? "insurance" : "banking";

                    // Iterate over each algorithm
                    for (String algorithm : algorithms) {

                    }

                } finally {
                    client.shutdown();
                }
            }
        } else {
            ClientAgg ClientAgg = new ClientAgg("localhost", 50051);
            ClientAgg.getRemoteDatasets("aaa", "insurance_v1.csv");

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

}

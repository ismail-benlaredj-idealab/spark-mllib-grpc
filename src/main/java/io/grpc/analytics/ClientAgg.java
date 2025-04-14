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


public class ClientAgg {

    private static final Logger logger = Logger.getLogger(Client.class.getName());

    private final ManagedChannel channel;
    private final DatasetAccessGrpc.DatasetAccessBlockingStub blockingStubFP;
    private final LinearRegressionGrpc.LinearRegressionBlockingStub blockingStubLinearRegression;

    /** Construct client connecting to server at {@code host:port}. */
    public ClientAgg(String host, int port) {
        channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext() // Note: For production, use proper authentication
                .build();
        blockingStubFP = DatasetAccessGrpc.newBlockingStub(channel);
        blockingStubLinearRegression = LinearRegressionGrpc.newBlockingStub(channel);
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
          
            String rootDirectory = "/home/ismail/grpc-java-examples-master/LR";
            String datasetPrefix = "anonymized_tcloseness";
            String outputPath = "/home/ismail/grpc-java-examples-master/anonymized_tcloseness_aggregated_features.csv";
            
            boolean success = LinearRegressionAgg.aggregateNodeResults(
                rootDirectory, 
                datasetPrefix, 
                outputPath
            );
            
            if (success) {
                System.out.println("Node feature importance aggregation completed successfully!");
            } else {
                System.out.println("Node feature importance aggregation did not complete successfully.");
            }

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

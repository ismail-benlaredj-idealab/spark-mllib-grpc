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

/**
 * A simple client that requests a file from the {@link GrpcServer}.
 */
public class Client {
    private static final Logger logger = Logger.getLogger(Client.class.getName());

    private final ManagedChannel channel;
    private final GreeterGrpc.GreeterBlockingStub blockingStub;
    private final FrequentItemsGrpc.FrequentItemsBlockingStub blockingStubFP;

    /** Construct client connecting to server at {@code host:port}. */
    public Client(String host, int port) {
        channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext() // Note: For production, use proper authentication
                .build();
        blockingStub = GreeterGrpc.newBlockingStub(channel);
        blockingStubFP= FrequentItemsGrpc.newBlockingStub(channel);
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
      

        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
        }
    }

   public void applyFpGrowth (String datasetName, String datasetPath, String outputPath){
    String mainPath = readSettings("DATASET_PATH");
    mainPath= Paths.get(mainPath).toAbsolutePath().toString();
    RequestFrequentItems request = RequestFrequentItems.newBuilder()
    .setDatasetPath(datasetPath)
    .setDatasetName(datasetName)
    .setOutputPath(outputPath)
    .build();
    try {
        // Ensure received_files directory exists
        File outputFolder = new File(readSettings("RECEIVED_FILES_PATH")+"");
        if (!outputFolder.exists()) {
            outputFolder.mkdirs();
        }
        // Get the response from the server
        ResponseFrequentItems response = blockingStubFP.ftGrowth(request);
        // Validate response
        if (response == null || response.getFileContent().isEmpty()) {
            logger.severe("No file content received from server");
            return;
        }
        // Sanitize filename
        String sanitizedFileName = response.getFileName();
        System.out.println(response.getFileName() );
        // Create output file
        File outputFile = new File(outputFolder, sanitizedFileName);
        // Write file content
      
    } catch (StatusRuntimeException e) {
        logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
    }



   }
    public static void main(String[] args) throws Exception {
       // Client client = new Client("localhost", 50051);
        // List of nodes to execute on
       // List<String> nodes = Arrays.asList("pe01-vm04", "pe01-vm05", "pe01-vm06", "pe02-vm04", "pe02-vm05", "pe02-vm06");
        List<String> nodes = Arrays.asList("pe01-vm06");

        // List of algorithms to run
        //List<String> algorithms = Arrays.asList("kanonymity", "ldiversity", "tcloseness");
        List<String> algorithms = Arrays.asList("kanonymity", "ldiversity", "tcloseness");
        for (String node : nodes) {
            Client client = new Client(node, 50051);
        
        try {
            String dataset_path = args.length > 0 ? args[0] : readSettings("DATASET_PATH");
            // client.applyAnalytics(dataset_path,"insurance", "kmeans");
           
            
            // Determine the dataset prefix based on the node (pe01 for banking, pe02 for insurance)
            String datasetPrefix = node.startsWith("pe02") ? "insurance" : "banking";
            
            // Iterate over each algorithm
            for (String algorithm : algorithms) {
                // Construct the dataset filename based on the algorithm
                String datasetFileName = "anonymized_" + algorithm + "_" + datasetPrefix + "_synthetic.csv";

                // Construct the dataset path
                String datasetPath = args.length > 0 
                    ? args[0] 
                    : "/home/" + node + readSettings("DATASET_PATH") + "/" + datasetFileName;	
                String outputPath="/home/" + node + readSettings("OUTPUT_PATH");
                // Execute the analytics operation
                //client.applyAnalytics(datasetPath, "banking_" + algorithm, "kmeans");
                System.out.println(datasetPath);
               client.applyFpGrowth(datasetFileName,datasetPath,outputPath);
               
               
            }
            
        } finally {
            client.shutdown();
        }
        }
    }


/********************* UTILS *********************************************************************/
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

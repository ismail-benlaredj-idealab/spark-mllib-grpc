package io.grpc.analytics;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Client {
    private static final Logger logger = Logger.getLogger(Client.class.getName());
    private final ManagedChannel channel;
    private final ClustringAnalysisGrpc.ClustringAnalysisBlockingStub blockingStub;
    private final FrequentItemsGrpc.FrequentItemsBlockingStub blockingStubFP;
    private final RandomForestGrpc.RandomForestBlockingStub blockingStubRandomForest;
    private final LinearRegressionGrpc.LinearRegressionBlockingStub blockingStubLinearRegression;

    public Client(String host, int port) {
        channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext() // Note: For production, use proper authentication
                .build();
        blockingStub = ClustringAnalysisGrpc.newBlockingStub(channel);
        blockingStubFP = FrequentItemsGrpc.newBlockingStub(channel);
        this.blockingStubRandomForest = RandomForestGrpc.newBlockingStub(channel);
        this.blockingStubLinearRegression = LinearRegressionGrpc.newBlockingStub(channel);
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

    public void applyFpGrowth( String datasetPath, String outputPath) {
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
    public static void main(String[] args) throws Exception {

        if(readSettings("MODE")=="PRODUCTION"){
          List<String> nodes = Arrays.asList("pe01-vm04", "pe01-vm05", "pe01-vm06",
            "pe02-vm04", "pe02-vm05", "pe02-vm06");
        

 
            for (String node : nodes) {
                Client client = new Client(node, 50051);

                try {
                    client.applyLinearRegression("/home/ismail/grpc-java-examples-master/datasets", "outputPath_LinearRegressionXXX");
                } finally {
                    client.shutdown();
                }
            }
        }else{
            Client client = new Client("localhost", 50051);
            //  client.applyAnalytics("/home/ismail/grpc-java-examples-master/datasets", "/home/ismail/grpc-java-examples-master/outputDataset");
            // client.applyFpGrowth("/home/ismail/grpc-java-examples-master/datasets",  "outputPath_FpGrowthXXX"); //// WE ADD TO THE PATH THE NODE NAME FROM THE FOR LOOP
           // client.applyRandomForest("/home/ismail/grpc-java-examples-master/datasets", "outputPath_RandomForestXXX");
              client.applyLinearRegression("/home/ismail/grpc-java-examples-master/datasets", "outputPath_LinearRegressionXXX");
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

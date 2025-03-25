package io.grpc.clustringkmeans;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A simple client that requests a greeting from the {@link GrpcServer}.
 */
public class Client {
    private static final Logger logger = Logger.getLogger(Client.class.getName());

    private final ManagedChannel channel;
    private final GreeterGrpc.GreeterBlockingStub blockingStub;

    /** Construct client connecting to HelloWorld server at {@code host:port}. */
    public Client(String host, int port) {
        channel = ManagedChannelBuilder.forAddress(host, port)
            .usePlaintext() // Note: usePlaintext(true) is deprecated
            .build();
        blockingStub = GreeterGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    /** Say hello to server. */
    public void applyAnalytics(String name) {
        logger.info("Will try to greet " + name + " ...");
        Request request = Request.newBuilder().setDatasetName(name).build();
        Response response;
        try {
            response = blockingStub.clustringKmeansServer(request);
            logger.info("Greeting: " + response.getMessage());
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
        }
    }
   public static String readSettings(String param) {
        String filePath = "/home/ismail/grpc-java-examples-master/src/main/java/io/grpc/clustringkmeans/resources/settings.dat";
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
    /**
     * Greet server. If provided, the first element of {@code args} is the name to use in the
     * greeting.
     */
    public static void main(String[] args) throws Exception {
        Client client = new Client("localhost", 50051);
        try {
            /* Access a service running on the local machine on port 50051 */
            String dataset_path = readSettings("DATASET_PATH");
            if (args.length > 0) {
                dataset_path = args[0]; /* Use the arg as the name to greet if provided */
            }
            client.applyAnalytics(dataset_path);
        } finally {
            client.shutdown();
        }
    }
}
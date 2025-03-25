package io.grpc.clustringkmeans;

import io.grpc.Server;
import io.grpc.clustringkmeans.GreeterGrpc;
import io.grpc.clustringkmeans.Request;
import io.grpc.clustringkmeans.Response;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.logging.Logger;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

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
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
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

    private static void clustringKmeans(String datasetPath){
          SparkConf conf = new SparkConf().setAppName("JavaKMeansExample").setMaster("local");
          JavaSparkContext jsc = new JavaSparkContext(conf);
          
          // Load and parse data
        //   String dataset_path = readSettings("DATASET_PATH");
          String dataset_path =datasetPath;
          String outputPath =  readSettings("OUTPUT_PATH");
          JavaRDD<String> data = jsc.textFile(dataset_path);
          
          // First pass: identify categorical columns and their possible values
          // Assume first line is header
          String header = data.first();
          String[] columns = header.split(",");
          int numColumns = columns.length;
          
          // Skip header for data processing
          JavaRDD<String> dataWithoutHeader = data.filter(line -> !line.equals(header));
          
          // Identify which columns are categorical and collect their unique values
          boolean[] isCategorical = new boolean[numColumns];
          Map<Integer, Set<String>> categoricalValues = new HashMap<>();
          Map<Integer, Map<String, Integer>> categoricalMappings = new HashMap<>();
          
          // First scan: determine which columns are categorical
          List<String[]> rows = dataWithoutHeader.map(line -> line.split(",")).collect();
          for (int i = 0; i < numColumns; i++) {
              boolean categorical = false;
              Set<String> uniqueValues = new HashSet<>();
              
              for (String[] row : rows) {
                  String value = row[i].trim();
                  uniqueValues.add(value);
                  try {
                      Double.parseDouble(value);
                  } catch (NumberFormatException e) {
                      categorical = true;
                  }
              }
              
              isCategorical[i] = categorical;
              if (categorical) {
                  categoricalValues.put(i, uniqueValues);
                  
                  // Create mapping for one-hot encoding
                  Map<String, Integer> valueMap = new HashMap<>();
                  int index = 0;
                  for (String value : uniqueValues) {
                      valueMap.put(value, index++);
                  }
                  categoricalMappings.put(i, valueMap);
              }
          }
          
          // Calculate the final vector size after one-hot encoding
          int vectorSize = 0;
          for (int i = 0; i < numColumns; i++) {
              if (isCategorical[i]) {
                  vectorSize += categoricalValues.get(i).size();
              } else {
                  vectorSize++;
              }
          }
          
          // Second pass: compute min and max for numerical columns for normalization
          double[] minValues = new double[numColumns];
          double[] maxValues = new double[numColumns];
          Arrays.fill(minValues, Double.MAX_VALUE);
          Arrays.fill(maxValues, Double.MIN_VALUE);
          
          for (String[] row : rows) {
              for (int i = 0; i < numColumns; i++) {
                  if (!isCategorical[i]) {
                      double value = Double.parseDouble(row[i].trim());
                      minValues[i] = Math.min(minValues[i], value);
                      maxValues[i] = Math.max(maxValues[i], value);
                  }
              }
          }
          
          // Find two numerical features for direct plotting
          List<Integer> numericalColumns = new ArrayList<>();
          for (int i = 0; i < numColumns; i++) {
              if (!isCategorical[i]) {
                  numericalColumns.add(i);
              }
          }
          
          // For direct plotting, use the first two numerical features if available
          int xAxisColumn = -1;
          int yAxisColumn = -1;
          
          if (numericalColumns.size() >= 2) {
              xAxisColumn = numericalColumns.get(0);
              yAxisColumn = numericalColumns.get(1);
              System.out.println("Selected columns for direct plotting: " + 
                                columns[xAxisColumn] + " and " + columns[yAxisColumn]);
          } else {
              System.out.println("Not enough numerical columns for direct plotting. Will use dimensionality reduction.");
          }
          
          // Convert data to feature vectors with one-hot encoding for categorical variables
          // Store the original data and transformation information for later use
          final boolean[] finalIsCategorical = isCategorical;
          final double[] finalMinValues = minValues;
          final double[] finalMaxValues = maxValues;
          final Map<Integer, Map<String, Integer>> finalCategoricalMappings = categoricalMappings;
          final Map<Integer, Set<String>> finalCategoricalValues = categoricalValues;
          
          // Convert to JavaRDD for processing
          JavaRDD<String[]> rowsRDD = jsc.parallelize(rows);
          
          // Create a mapping of raw data to feature vectors
          final int xx =vectorSize;
          JavaRDD<Vector> parsedData = rowsRDD.map(values -> {
              double[] features = new double[xx];
              
              int featureIndex = 0;
              for (int i = 0; i < values.length; i++) {
                  String value = values[i].trim();
                  
                  if (finalIsCategorical[i]) {
                      // One-hot encoding
                      Map<String, Integer> valueMap = finalCategoricalMappings.get(i);
                      int oneHotIndex = valueMap.get(value);
                      for (int j = 0; j < finalCategoricalValues.get(i).size(); j++) {
                          features[featureIndex++] = (j == oneHotIndex) ? 1.0 : 0.0;
                      }
                  } else {
                      // Normalize numerical values
                      double normalizedValue = (Double.parseDouble(value) - finalMinValues[i]) / 
                                               (finalMaxValues[i] - finalMinValues[i]);
                      features[featureIndex++] = normalizedValue;
                  }
              }
              
              return Vectors.dense(features);
          });
          
          parsedData.cache();
          
          // Cluster the data using KMeans
          int numClusters = Integer.parseInt(readSettings("CLUSTERS_NUMBERS"));
          int numIterations = Integer.parseInt(readSettings("K"));
          KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);
          
          // Predict clusters for all data points
          List<Vector> allPoints = parsedData.collect();
          List<Integer> allPredictions = new ArrayList<>();
          
          for (Vector point : allPoints) {
              allPredictions.add(clusters.predict(point));
          }
          
          // Prepare 2D coordinates for plotting
          double[][] plotCoordinates = new double[rows.size()][2];
          
          if (xAxisColumn >= 0 && yAxisColumn >= 0) {
              // Use selected numerical features directly
              for (int i = 0; i < rows.size(); i++) {
                  double xValue = Double.parseDouble(rows.get(i)[xAxisColumn].trim());
                  double yValue = Double.parseDouble(rows.get(i)[yAxisColumn].trim());
                  
                  plotCoordinates[i][0] = xValue;
                  plotCoordinates[i][1] = yValue;
              }
          } else {
              // Instead of PCA, select two prominent features or use a simplified approach
              // For now, we'll use a simple approach: project to the first two dimensions
              // In a real application, you might want to use a more sophisticated dimensionality reduction
              
              // Simplified approach: find two dimensions with the highest variance
              double[] variances = new double[vectorSize];
              double[] means = new double[vectorSize];
              
              // Calculate means
              for (Vector point : allPoints) {
                  for (int i = 0; i < vectorSize; i++) {
                      means[i] += point.apply(i);
                  }
              }
              for (int i = 0; i < vectorSize; i++) {
                  means[i] /= allPoints.size();
              }
              
              // Calculate variances
              for (Vector point : allPoints) {
                  for (int i = 0; i < vectorSize; i++) {
                      variances[i] += Math.pow(point.apply(i) - means[i], 2);
                  }
              }
              for (int i = 0; i < vectorSize; i++) {
                  variances[i] /= allPoints.size();
              }
              
              // Find two dimensions with highest variance
              int dim1 = 0;
              int dim2 = 1;
              for (int i = 2; i < vectorSize; i++) {
                  if (variances[i] > variances[dim1]) {
                      dim2 = dim1;
                      dim1 = i;
                  } else if (variances[i] > variances[dim2]) {
                      dim2 = i;
                  }
              }
              
              // Project to these two dimensions
              for (int i = 0; i < allPoints.size(); i++) {
                  plotCoordinates[i][0] = allPoints.get(i).apply(dim1);
                  plotCoordinates[i][1] = allPoints.get(i).apply(dim2);
              }
              
              System.out.println("Using dimensions with highest variance for 2D projection");
          }
          
          // Write the results to a CSV file suitable for plotting
          try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath))) {
              // Write header
              if (xAxisColumn >= 0 && yAxisColumn >= 0) {
                  writer.write(columns[xAxisColumn] + "," + columns[yAxisColumn] + ",cluster\n");
              } else {
                  writer.write("Dimension1,Dimension2,cluster\n");
              }
              
              // Write data rows with cluster assignments and 2D coordinates for plotting
              for (int i = 0; i < rows.size(); i++) {
                  StringBuilder sb = new StringBuilder();
                  
                  // Write 2D coordinates
                  sb.append(plotCoordinates[i][0]).append(",");
                  sb.append(plotCoordinates[i][1]).append(",");
                  
                  // Add cluster assignment
                  sb.append(allPredictions.get(i));
                  sb.append("\n");
                  
                  writer.write(sb.toString());
              }
              
              System.out.println("Cluster assignments with 2D coordinates saved to " + outputPath);
          } catch (IOException e) {
              System.err.println("Error writing to CSV file: " + e.getMessage());
              e.printStackTrace();
          }
          
          // Write a second file with complete data for further analysis
          try (BufferedWriter writer = new BufferedWriter(new FileWriter("complete_cluster_assignments.csv"))) {
              // Write header with additional columns for plotting coordinates
              if (xAxisColumn >= 0 && yAxisColumn >= 0) {
                  writer.write(header + ",plot_x,plot_y,cluster\n");
              } else {
                  writer.write(header + ",dim1,dim2,cluster\n");
              }
              
              // Write data rows with cluster assignments
              for (int i = 0; i < rows.size(); i++) {
                  StringBuilder sb = new StringBuilder();
                  
                  // Write original data values
                  for (int j = 0; j < rows.get(i).length; j++) {
                      sb.append(rows.get(i)[j]);
                      sb.append(",");
                  }
                  
                  // Add plotting coordinates
                  sb.append(plotCoordinates[i][0]).append(",");
                  sb.append(plotCoordinates[i][1]).append(",");
                  
                  // Add cluster assignment
                  sb.append(allPredictions.get(i));
                  sb.append("\n");
                  
                  writer.write(sb.toString());
              }
              
          } catch (IOException e) {
              System.err.println("Error writing to CSV file: " + e.getMessage());
              e.printStackTrace();
          }
          
          // Output to console
          System.out.println("\nCluster centers:");
          for (Vector center : clusters.clusterCenters()) {
              System.out.println(" " + center);
          }
          
          double cost = clusters.computeCost(parsedData.rdd());
          System.out.println("Within Set Sum of Squared Errors = " + cost);
          
          jsc.stop();
        }

         public static void writeExecutionTimeToCSV(String csvFile, double executionTimeInSeconds) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(csvFile, true))) {
            writer.write("Excution Time"+ "," + executionTimeInSeconds + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static class GreeterImpl extends GreeterGrpc.GreeterImplBase {
        @Override
        public void clustringKmeansServer(Request req, StreamObserver<Response> responseObserver) {
            Response reply = Response.newBuilder().setMessage("Hello " + req.getDatasetName()).build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
            System.out.println("***************************** RUN CODE HERE *****************************************************");
            long startTime = System.nanoTime();
            clustringKmeans(req.getDatasetName());

            long endTime = System.nanoTime();

            double executionTimeInSeconds = (endTime - startTime) / 1_000_000_000.0;
            
            writeExecutionTimeToCSV("executionTime", executionTimeInSeconds);

    }
}
    
        /**
         * Main launches the server from the command line.
         */
        public static void main(String[] args) throws IOException, InterruptedException {
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
            server.blockUntilShutdown();
        }
    
      
}



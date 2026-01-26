package io.grpc.analytics;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.analytics.LinearRegressionAnalytics.LinearRegressionResult;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.channels.Pipe;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.Transformer;

import com.google.protobuf.ByteString;

public class ClientAgg {

    private static final Logger logger = Logger.getLogger(Client.class.getName());

    private final ManagedChannel channel;
    private final LinearRegressionGrpc.LinearRegressionBlockingStub blockingStubLinearRegression;
    private final AnonymityServiceGrpc.AnonymityServiceBlockingStub blockingStubAA;
    private final ClustringAnalysisGrpc.ClustringAnalysisBlockingStub blockingStubKMeans;
    private final DatasetAccessGrpc.DatasetAccessBlockingStub blockingStubDatasetAccess;
    private final RandomForestGrpc.RandomForestBlockingStub blockingStubRandomForest;

    /** Construct client connecting to server at {@code host:port}. */
    public ClientAgg(String host, int port) {
        channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext() // Note: For production, use proper authentication
                .build();
        blockingStubLinearRegression = LinearRegressionGrpc.newBlockingStub(channel);
        blockingStubAA = AnonymityServiceGrpc.newBlockingStub(channel);
        blockingStubKMeans = ClustringAnalysisGrpc.newBlockingStub(channel);
        blockingStubDatasetAccess = DatasetAccessGrpc.newBlockingStub(channel);
        this.blockingStubRandomForest = RandomForestGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public void getRemoteDatasets(String node, String folderPath, String outputFolderPath) {
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
                String fileName = node + "_" + fileData.getFileName();
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

    public void triggerAACalculation(String originalPath, String anonymizedPath, List<String> qiColumns,
            String resultsPath) {
        logger.info("Will try to trigger AA calculation...");
        AARequest request = AARequest.newBuilder()
                .setOriginalCsvPath(originalPath)
                .setAnonymizedCsvPath(anonymizedPath)
                .addAllQuasiIdentifierColumns(qiColumns)
                .setOutputResultsPath(resultsPath)
                .build();

        try {
            AAResponse response = blockingStubAA.calculateAA(request);
            if (response.getStatus() == AAResponse.Status.SUCCESS) {
                logger.info("✅ RPC Success: " + response.getMessage());
            } else {
                logger.severe("❌ RPC Error: " + response.getMessage());
            }
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
        }
    }

    public void applyAnalytics(String datasetPath, String outputPath) {
        RequestClustringKmeans request = RequestClustringKmeans.newBuilder()
                .setDatasetPath(datasetPath)
                .setOutputPath(outputPath)
                .build();
        try {
            ResponseClustringKmeans response = blockingStubKMeans.clustringKmeansServer(request);
            System.out.println("Kmeans Response: " + response.toString());
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

    public static void main(String[] args) throws Exception {

        String MODE = "dev"; // dev or prod

        if (MODE == "prod") {

            // List<String> nodes = Arrays.asList("pe01-vm04", "pe01-vm05", "pe01-vm06",
            // "pe02-vm04", "pe02-vm05", "pe02-vm06");
            List<String> nodes = Arrays.asList("pe01-vm03", "pe01-vm06");

            // ClusteringCloudMode(nodes);

            long start = 0;
            for (String node : nodes) {
                String NodeDir = "/home/" + node + "/Documents/spark-mllib-grpc-dev";
                ClientAgg ClientAgg = new ClientAgg(node, 50051);
                start = System.currentTimeMillis();

                String datasetPath = NodeDir + "/home/ismail/grpc-java-examples-master/clustering/bank_500.csv";
                String outputPath = NodeDir + "/LR";

                SparkSession spark = SparkSession.builder()
                        .appName("Linear Regression")
                        .master("local[*]") // Use local mode for testing
                        .getOrCreate();
                LinearRegressionAnalytics LRAnalysis = new LinearRegressionAnalytics(spark, datasetPath, outputPath);
                LRAnalysis.runAnalysis();
                spark.stop();

                ClientAgg.shutdown();

            }

        } else {

            ClientAgg ClientAgg = new ClientAgg("localhost", 50051);

            String datasetPath = "/home/ismail/grpc-java-examples-master/clustring/bank_500.csv";
            String outputPath = "/home/ismail/grpc-java-examples-master/LR";

            SparkSession spark = SparkSession.builder()
                    .appName("Linear Regression")
                    .master("local[*]") // Use local mode for testing
                    .getOrCreate();
            // LinearRegressionAnalytics LRAnalysis = new LinearRegressionAnalytics(spark,
            // datasetPath, outputPath);
            // LinearRegressionResult dd = LRAnalysis.runAnalysis();

            // List<Long> trainingSizes = Arrays.asList(dd.getTrainingDataSize(),
            // dd.getTrainingDataSize());
            // List<String> modelPaths =
            // Arrays.asList("/home/ismail/grpc-java-examples-master/received_files/modelA",
            // "/home/ismail/grpc-java-examples-master/received_files/modelB");
            // PipelineModel aggregatedModel = weightedAggregation(modelPaths,
            // trainingSizes);

            // // Save the aggregated model
            // aggregatedModel.write().overwrite()
            // .save("/home/ismail/grpc-java-examples-master/outputDataset/model_aggregated");

            // String modelPath =
            // "/home/ismail/grpc-java-examples-master/outputDataset/model_aggregated";

            // evaluateModel(spark, dd.getTestData(), modelPath);

            ClientAgg.applyRandomForest("/home/ismail/grpc-java-examples-master/clustring/bank_500.csv",
                    "outputPath_RandomForestXXX");

            // ************************************************************ */
            //
            //
            //
            //
            spark.stop();

            ClientAgg.shutdown();
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

    public static List<List<String>> readQuasiIdentifiersFromFile(String filePath) throws IOException {
        List<List<String>> quasiSets = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (!line.isEmpty()) {
                    List<String> identifiers = new ArrayList<>();
                    String[] parts = line.split(",");
                    for (String part : parts) {
                        identifiers.add(part.trim());
                    }
                    quasiSets.add(identifiers);
                }
            }
        }
        return quasiSets;
    }

    public static void aggregateCSVFiles(String folderPath, String outputPath) throws IOException {
        File folder = new File(folderPath);
        File[] csvFiles = folder.listFiles((dir, name) -> name.toLowerCase().endsWith(".csv"));

        if (csvFiles == null || csvFiles.length == 0) {
            throw new IOException("No CSV files found in the specified folder");
        }

        List<String[]> aggregatedData = new ArrayList<>();
        String[] headers = null;
        int fileCount = 0;

        for (File file : csvFiles) {
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                String line;
                int rowIndex = 0;

                while ((line = reader.readLine()) != null) {
                    String[] values = line.split(",");

                    if (rowIndex == 0) {
                        // Handle headers
                        if (headers == null) {
                            headers = values.clone();
                            // Initialize aggregated data structure
                            aggregatedData.add(headers);
                        }
                    } else {
                        // Handle data rows
                        if (fileCount == 0) {
                            // First file: initialize with its values
                            String[] row = new String[values.length];
                            for (int i = 0; i < values.length - 1; i++) {
                                row[i] = values[i]; // Copy non-numeric columns as is
                            }
                            row[values.length - 1] = values[values.length - 1]; // Last column value
                            aggregatedData.add(row);
                        } else {
                            // Subsequent files: add to existing values
                            if (rowIndex < aggregatedData.size()) {
                                String[] existingRow = aggregatedData.get(rowIndex);
                                double existingValue = Double.parseDouble(existingRow[existingRow.length - 1]);
                                double newValue = Double.parseDouble(values[values.length - 1]);
                                existingRow[existingRow.length - 1] = String.valueOf(existingValue + newValue);
                            }
                        }
                    }
                    rowIndex++;
                }
            }
            fileCount++;
        }

        // Calculate averages (divide by number of files)
        for (int i = 1; i < aggregatedData.size(); i++) {
            String[] row = aggregatedData.get(i);
            double sum = Double.parseDouble(row[row.length - 1]);
            double average = sum / fileCount;
            row[row.length - 1] = String.format("%.6f", average);
        }

        // Write results to output file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath))) {
            for (String[] row : aggregatedData) {
                writer.write(String.join(",", row));
                writer.newLine();
            }
        }
    }

    public static void cleanCSV(String filePath, int expectedColumns) throws IOException {
        // Read the file and clean its content
        List<String> cleanedLines = new ArrayList<>();

        try (BufferedReader reader = Files.newBufferedReader(Paths.get(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim(); // Remove leading/trailing spaces

                // Skip empty lines
                if (line.isEmpty()) {
                    continue;
                }

                // Split the line into columns
                String[] columns = line.split(",");

                // Check if the line has the expected number of columns
                if (columns.length == expectedColumns) {
                    // Trim each column (field) to remove extra spaces
                    for (int i = 0; i < columns.length; i++) {
                        columns[i] = columns[i].trim();
                    }

                    // Reconstruct the line and add it to cleanedLines
                    String cleanedLine = String.join(",", columns);
                    cleanedLines.add(cleanedLine);
                } else {
                    // Skip lines that do not have the expected number of columns
                    System.out.println("Skipping invalid line: " + line);
                }
            }
        }

        // Now overwrite the original file with the cleaned content
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(filePath))) {
            for (String cleanedLine : cleanedLines) {
                writer.write(cleanedLine);
                writer.newLine();
            }
        }
    }

    public static void logExecutionTime(long startTime, long endTime, String datasetPath, String resultsPath) {
        long executionTime = endTime - startTime;
        int tuplesNumber = getDatasetRowCount(datasetPath);

        try {
            // Create results file with header if it doesn't exist
            if (!Files.exists(Paths.get(resultsPath))) {
                try (FileWriter writer = new FileWriter(resultsPath)) {
                    writer.append("tuplesNumber,executionTime\n");
                }
            }

            // Append new record to results CSV
            try (FileWriter writer = new FileWriter(resultsPath, true)) {
                writer.append(tuplesNumber + "," + executionTime + "\n");
            }

            System.out.println("Execution time logged successfully: "
                    + tuplesNumber + " tuples, "
                    + executionTime + " ms");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static int getDatasetRowCount(String datasetPath) {
        int count = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(datasetPath))) {
            while (br.readLine() != null) {
                count++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        // subtract 1 for header
        return (count > 0) ? count - 1 : 0;
    }

    public static List<String> getCsvFiles(String dirPath) {
        List<String> csvFiles = new ArrayList<>();
        File directory = new File(dirPath);

        if (directory.exists() && directory.isDirectory()) {
            File[] files = directory.listFiles((dir, name) -> name.toLowerCase().endsWith(".csv"));
            if (files != null) {
                for (File file : files) {
                    csvFiles.add(file.getAbsolutePath());
                }
            }
        } else {
            System.err.println("Invalid directory: " + dirPath);
        }

        return csvFiles;
    }

    // public static Dataset<Row> ensemblePredictions(Dataset<Row> testData,
    // List<PipelineModel> models) {
    // Dataset<Row> result = null;

    // for (int i = 0; i < models.size(); i++) {
    // Dataset<Row> predictions = models.get(i).transform(testData);

    // if (i == 0) {
    // result = predictions.withColumnRenamed("prediction", "pred_0");
    // } else {
    // result = result.join(
    // predictions.select("features", "prediction").withColumnRenamed("prediction",
    // "pred_" + i),
    // "features");
    // }
    // }

    // // Average all predictions
    // String[] predCols = new String[models.size()];
    // for (int i = 0; i < models.size(); i++) {
    // predCols[i] = "pred_" + i;
    // }

    // result = result.withColumn("ensemble_prediction",
    // functions.expr("(" + String.join(" + ", predCols) + ") / " + models.size()));

    // return result;
    // }

    public static Dataset<Row> evaluateModel(SparkSession spark, Dataset<Row> testData, String modelPath) {
        // Load the trained model
        PipelineModel model = PipelineModel.load(modelPath);

        // Make predictions on test data
        Dataset<Row> predictions = model.transform(testData);

        // Create evaluators for different metrics
        RegressionEvaluator rmseEvaluator = new RegressionEvaluator()
                .setLabelCol("Point Earned")
                .setPredictionCol("prediction")
                .setMetricName("rmse");

        RegressionEvaluator r2Evaluator = new RegressionEvaluator()
                .setLabelCol("Point Earned")
                .setPredictionCol("prediction")
                .setMetricName("r2");

        RegressionEvaluator maeEvaluator = new RegressionEvaluator()
                .setLabelCol("Point Earned")
                .setPredictionCol("prediction")
                .setMetricName("mae");

        RegressionEvaluator mseEvaluator = new RegressionEvaluator()
                .setLabelCol("Point Earned")
                .setPredictionCol("prediction")
                .setMetricName("mse");

        // Calculate all metrics
        double rmse = rmseEvaluator.evaluate(predictions);
        double r2 = r2Evaluator.evaluate(predictions);
        double mae = maeEvaluator.evaluate(predictions);
        double mse = mseEvaluator.evaluate(predictions);

        // Create a DataFrame to store the model results
        List<Row> resultRows = new ArrayList<>();
        resultRows.add(RowFactory.create("RMSE", rmse));
        resultRows.add(RowFactory.create("R²", r2));
        resultRows.add(RowFactory.create("Mean Absolute Error", mae));
        resultRows.add(RowFactory.create("Mean Squared Error", mse));

        // Create the results DataFrame
        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("Metric", DataTypes.StringType, false),
                DataTypes.createStructField("Value", DataTypes.DoubleType, false)
        });

        Dataset<Row> resultsDF = spark.createDataFrame(resultRows, schema);
        resultsDF.show();

        return resultsDF;
    }

    public static PipelineModel weightedAggregation(
            List<String> modelPaths,
            List<Long> trainingSizes) {

        List<LinearRegressionModel> models = new ArrayList<>();
        PipelineModel referencePipeline = null;

        for (String path : modelPaths) {
            PipelineModel pipeline = PipelineModel.load(path);
            if (referencePipeline == null) {
                referencePipeline = pipeline;
            }
            LinearRegressionModel lrModel = (LinearRegressionModel) pipeline.stages()[pipeline.stages().length - 1];
            models.add(lrModel);
        }

        long totalSamples = trainingSizes.stream().mapToLong(Long::longValue).sum();

        // Weighted average based on training data size
        int numFeatures = models.get(0).coefficients().size();
        double[] weightedCoeffs = new double[numFeatures];
        double weightedIntercept = 0.0;

        for (int i = 0; i < models.size(); i++) {
            double weight = (double) trainingSizes.get(i) / totalSamples;

            double[] coeffs = models.get(i).coefficients().toArray();
            for (int j = 0; j < numFeatures; j++) {
                weightedCoeffs[j] += coeffs[j] * weight;
            }

            weightedIntercept += models.get(i).intercept() * weight;
        }

        LinearRegressionModel aggregatedLR = new LinearRegressionModel(
                "weighted_aggregated_model",
                Vectors.dense(weightedCoeffs),
                weightedIntercept);

        // Create new pipeline with aggregated model
        Transformer[] newStages = referencePipeline.stages().clone();
        newStages[newStages.length - 1] = aggregatedLR;

        return new PipelineModel("aggregated_pipeline", newStages);
    }

    /****** CLOUD MODE */
    /********************************** */
    public static void ClusteringCloudMode(List<String> nodes) throws Exception {
        long start = 0;

        // Process each node
        for (String node : nodes) {
            ClientAgg ClientAgg = new ClientAgg(node, 50051);
            start = System.currentTimeMillis();
            ClientAgg.applyAnalytics("/home/" + node + "/Documents/spark-mllib-grpc-dev/clustring/bank_3000.csv",
                    "/home/" + node + "/Documents/spark-mllib-grpc-dev/outputDataset");
            ClientAgg.getRemoteDatasets(
                    "/home/" + node + "/Documents/spark-mllib-grpc-dev/outputDataset/" + node
                            + "_kmeans_bank_3000.csv",
                    "/home/" + System.getProperty("user.name") + "/Documents/spark-mllib-grpc-dev/received_files");
        }

        // Initialize Spark
        SparkConf conf = new SparkConf()
                .setAppName("ClusterOfClusters")
                .setMaster("local[*]"); // Use local mode for testing
        JavaSparkContext jsc = new JavaSparkContext(conf);

        // List of dataset file paths (CSV files)
        List<String> receivedPaths = getCsvFiles(
                "/home/" + System.getProperty("user.name") + "/Documents/spark-mllib-grpc-dev/received_files");

        // Output directory
        String outputDir = "/home/" + System.getProperty("user.name")
                + "/Documents/spark-mllib-grpc-dev/clusterComb/kmeans_bank_3000_clusterOfclusters.csv";

        // Number of clusters and iterations
        int numClusters = 5;
        int numIterations = 20;

        // Create the clustering object
        ClusterAgg_V1 clustering = new ClusterAgg_V1(
                jsc,
                receivedPaths,
                outputDir,
                numClusters,
                numIterations);

        // Run clustering
        clustering.runClustering();

        long end = System.currentTimeMillis();
        logExecutionTime(start, end,
                "/home/" + System.getProperty("user.name") + "/Documents/spark-mllib-grpc-dev/received_files/"
                        + nodes.get(0) + "_kmeans_bank_3000.csv",
                "/home/" + System.getProperty("user.name")
                        + "/Documents/spark-mllib-grpc-dev/clusterComb/executionTime.csv");

        // Stop Spark
        jsc.close();
    }

    public static void AnonymizationAccuracyCloudMode(String message) {
        // for (String node : nodes) {
        // ClientAgg ClientAgg = new ClientAgg(node, 50051);

        // try {
        // String quasiIdentifierFile =
        // "/home/pe01-vm05/Documents/spark-mllib-grpc-dev/datasets/quasi_identifiers.dat";
        // List<List<String>> allQuasiIdentifiers =
        // readQuasiIdentifiersFromFile(quasiIdentifierFile);
        // String resultsFile = "/home/" + node +
        // "/Documents/spark-mllib-grpc-dev/datasets/AARes.csv";

        // for (int i = 2; i < 11; i++) {
        // try {
        // System.out.println("Processing round " + (i) + " with quasi-identifiers: "
        // + allQuasiIdentifiers.get(i));
        // String originalFile = "/home/" + node
        // + "/Documents/spark-mllib-grpc-dev/datasets/banking_synthetic_v1.csv";
        // String anonymizedFile = "/home/" + node
        // + "/Documents/spark-mllib-grpc-dev/datasets/anonymized_bank_A" + (i) +
        // ".csv";

        // // we -2 because we have the same index
        // // for datasets A2, A4... A2 is the first dataset set but i=2 is the 4th line
        // in
        // // the qusi identifires
        // List<String> quasiIdentifiers = allQuasiIdentifiers.get(i - 2);

        // System.out.println(
        // "Processing round " + (i + 1) + " with quasi-identifiers: " +
        // quasiIdentifiers);

        // // --- Trigger the new service and get the score ---

        // ClientAgg.triggerAACalculation(originalFile, anonymizedFile,
        // quasiIdentifiers, resultsFile);

        // } catch (Exception e) {
        // System.out.println("Error processing round " + (i + 1) + ": " +
        // e.getMessage());
        // e.printStackTrace();
        // }
        // }

        // ClientAgg.getRemoteDatasets(node,
        // "/home/" + node + "/Documents/spark-mllib-grpc-dev/datasets/AARes.csv",
        // "/home/pe01-vm05/Documents/spark-mllib-grpc-dev/received_files");

        // } finally {
        // ClientAgg.shutdown();
        // }
        // }

        // File outputFolder = new
        // File("/home/pe01-vm05/Documents/spark-mllib-grpc-dev/datasets/agg_results.csv");
        // if (outputFolder.exists()) {
        // cleanCSV("/home/pe01-vm05/Documents/spark-mllib-grpc-dev/datasets/agg_results.csv",
        // 2);
        // }

        // aggregateCSVFiles("/home/pe01-vm05/Documents/spark-mllib-grpc-dev/received_files",
        // "/home/pe01-vm05/Documents/spark-mllib-grpc-dev/datasets/agg_results.csv");
    }

    /*********** LOCAL MODE */
    public static void ClusteringLocalMode(String message) {
        // long start = System.currentTimeMillis();
        // ClientAgg.applyAnalytics("/home/ismail/grpc-java-examples-master/clustring/bank_3000.csv",
        // "/home/ismail/grpc-java-examples-master/outputDataset");
        // ClientAgg.getRemoteDatasets(
        // "/home/ismail/grpc-java-examples-master/outputDataset/ismail_kmeans_bank_3000.csv",
        // "/home/ismail/grpc-java-examples-master/received_files");

        // SparkConf conf = new SparkConf()
        // .setAppName("ClusterOfClusters")
        // .setMaster("local[*]"); // Use local mode for testing
        // JavaSparkContext jsc = new JavaSparkContext(conf);

        // // List of dataset file paths (CSV files

        // List<String> receivedPaths =
        // getCsvFiles("/home/ismail/grpc-java-examples-master/received_files");

        // // Output directory
        // String outputDir =
        // "/home/ismail/grpc-java-examples-master/clusterComb/ismail_kmeans_bank_3000_clusterOfclusters.csv";

        // // Number of clusters and iterations
        // int numClusters = 5;
        // int numIterations = 20;

        // // Create the clustering object
        // ClusterAgg_V1 clustering = new ClusterAgg_V1(
        // jsc,
        // receivedPaths,
        // outputDir,
        // numClusters,
        // numIterations);

        // // Run clustering
        // clustering.runClustering();
        // long end = System.currentTimeMillis();
        // logExecutionTime(start, end,
        // "/home/ismail/grpc-java-examples-master/received_files/ismail_kmeans_bank_3000.csv",
        // "/home/ismail/grpc-java-examples-master/clusterComb/executionTime.csv");
        // Stop Spark
        // jsc.close();
    }

    public static void AnonymizationAccuracyLocalMode(String message) {
        // String quasiIdentifierFile =
        ////////////////////////////////////////////////////////////// "/home/ismail/grpc-java-examples-master/datasets/quasi_identifiers.dat";
        // List<List<String>> allQuasiIdentifiers =
        ////////////////////////////////////////////////////////////// readQuasiIdentifiersFromFile(quasiIdentifierFile);
        // String resultsFile =
        ////////////////////////////////////////////////////////////// "/home/ismail/grpc-java-examples-master/datasets/AARes.csv";

        // for (int i = 2; i < 11; i++) {
        // try {
        // System.out.println("Processing round " + (i ) + " with quasi-identifiers: "
        // + allQuasiIdentifiers.get(i));
        // String originalFile =
        // "/home/ismail/grpc-java-examples-master/datasets/banking_synthetic_v1.csv";
        // String anonymizedFile =
        // "/home/ismail/grpc-java-examples-master/datasets/anonymized_bank_A"+(i)+".csv";
        // List<String> quasiIdentifiers = allQuasiIdentifiers.get(i);

        // System.out.println("Processing round " + (i + 1) + " with quasi-identifiers:
        // " + quasiIdentifiers);

        // // --- Trigger the new service and get the score ---
        // ClientAgg.triggerAACalculation(originalFile, anonymizedFile,
        // quasiIdentifiers, resultsFile);

        // } catch (Exception e) {
        // System.out.println("Error processing round " + (i + 1) + ": " +
        // e.getMessage());
        // e.printStackTrace();
        // }
        // }

        // ClientAgg.getRemoteDatasets("","/home/ismail/grpc-java-examples-master/datasets/AARes.csv",
        // "/home/ismail/grpc-java-examples-master/received_files");

        // ClientAgg.aggregateCSVFiles("/home/ismail/grpc-java-examples-master/received_files",
        // "/home/ismail/grpc-java-examples-master/datasets/agg_results.csv");
    }

}

package io.grpc.analytics;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BatchKMeansClusteringAnalytics implements Serializable {

    private static final long serialVersionUID = 1L;
    
    private final transient JavaSparkContext jsc;
    private final transient SparkSession spark;
    private final String datasetPath;
    private final String outputDir;
    private final String datasetName;
    private final int numClusters;
    private final int numBatches;
    private final int numIterations;
    private final long randomSeed;

    /**
     * Constructor for the BatchKMeansClusteringAnalytics class.
     * 
     * @param jsc           The JavaSparkContext to use for the analysis
     * @param datasetPath   The path to the input dataset
     * @param outputDir     The directory to save the output results
     * @param datasetName   The name of the dataset for output file naming
     * @param numClusters   The number of clusters to create
     * @param numBatches    The number of batches to process the data in
     * @param numIterations The number of iterations for K-Means training
     * @param randomSeed    The random seed for reproducibility
     */
    public BatchKMeansClusteringAnalytics(
            JavaSparkContext jsc,
            String datasetPath,
            String outputDir,
            String datasetName,
            int numClusters,
            int numBatches,
            int numIterations,
            long randomSeed) {
        this.jsc = jsc;
        this.spark = SparkSession.builder().config(jsc.getConf()).getOrCreate();
        this.datasetPath = datasetPath;
        this.outputDir = outputDir;
        this.datasetName = datasetName;
        this.numClusters = numClusters;
        this.numBatches = numBatches;
        this.numIterations = numIterations;
        this.randomSeed = randomSeed;
    }

    /**
     * Run batch K-means clustering on the provided dataset.
     * 
     * @return A summary of the clustering results
     * @throws IOException If an error occurs during analysis
     */
    public BatchClusteringResult runBatchClustering() throws IOException {
        // Load and preprocess data
        Dataset<Row> data = loadAndPreprocessData();
        
        // Split data into batches
        Dataset<Row>[] batches = data.randomSplit(createSplitRatios(), randomSeed);
        
        List<KMeansModel> batchModels = new ArrayList<>();
        List<Dataset<Row>> batchResults = new ArrayList<>();
        
        // Process each batch
        for (int i = 0; i < numBatches; i++) {
            System.out.println("Processing batch " + (i + 1) + " of " + numBatches);
            
            // Train K-Means on this batch
            KMeans kmeans = new KMeans()
                .setK(numClusters)
                .setMaxIter(numIterations)
                .setSeed(randomSeed + i);
            
            KMeansModel model = kmeans.fit(batches[i]);
            batchModels.add(model);
            
            // Get predictions for this batch
            Dataset<Row> predictions = model.transform(batches[i])
                .withColumn("batch", org.apache.spark.sql.functions.lit(i));
            
            batchResults.add(predictions);
            
            System.out.println("Batch " + (i + 1) + " completed. Cluster centers:");
            for (int j = 0; j < model.clusterCenters().length; j++) {
                System.out.println(" Cluster " + j + ": " + model.clusterCenters()[j]);
            }
        }
        
        // Combine all batch results
        Dataset<Row> allResults = batchResults.get(0);
        for (int i = 1; i < batchResults.size(); i++) {
            allResults = allResults.union(batchResults.get(i));
        }
        
        // Train a final model on the entire dataset
        KMeans finalKmeans = new KMeans()
            .setK(numClusters)
            .setMaxIter(numIterations)
            .setSeed(randomSeed);
        
        KMeansModel finalModel = finalKmeans.fit(data);
        double finalCost = finalModel.summary().trainingCost();
        
        // Write results to file
        String outputFilePath = writeResultsToFile(allResults);
        
        System.out.println("\nFinal cluster centers:");
        for (int i = 0; i < finalModel.clusterCenters().length; i++) {
            System.out.println(" Cluster " + i + ": " + finalModel.clusterCenters()[i]);
        }
        
        System.out.println("Final Within Set Sum of Squared Errors = " + finalCost);
        
        return new BatchClusteringResult(
            finalModel,
            finalCost,
            outputFilePath,
            batchModels,
            numBatches
        );
    }

    /**
     * Load and preprocess the data.
     */
    private Dataset<Row> loadAndPreprocessData() {
        // Load data
        Dataset<Row> data = spark.read()
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(datasetPath);
        
        // Get column names and types
        StructField[] fields = data.schema().fields();
        List<String> featureCols = new ArrayList<>();
        List<String> categoricalCols = new ArrayList<>();
        List<String> numericalCols = new ArrayList<>();
        
        for (StructField field : fields) {
            String colName = field.name();
            if (field.dataType() == DataTypes.StringType) {
                categoricalCols.add(colName);
            } else if (field.dataType() == DataTypes.DoubleType || 
                       field.dataType() == DataTypes.IntegerType ||
                       field.dataType() == DataTypes.FloatType ||
                       field.dataType() == DataTypes.LongType) {
                numericalCols.add(colName);
            }
        }
        
        // Process categorical columns
        List<StringIndexer> indexers = new ArrayList<>();
        List<OneHotEncoder> encoders = new ArrayList<>();
        List<String> encodedCols = new ArrayList<>();
        
        for (String col : categoricalCols) {
            StringIndexer indexer = new StringIndexer()
                .setInputCol(col)
                .setOutputCol(col + "_index")
                .setHandleInvalid("skip");
            indexers.add(indexer);
            
            OneHotEncoder encoder = new OneHotEncoder()
                .setInputCol(col + "_index")
                .setOutputCol(col + "_encoded");
            encoders.add(encoder);
            
            encodedCols.add(col + "_encoded");
        }
        
        // Apply string indexing
        Dataset<Row> indexedData = data;
        for (StringIndexer indexer : indexers) {
            indexedData = indexer.fit(indexedData).transform(indexedData);
        }
        
        // Apply one-hot encoding
        Dataset<Row> encodedData = indexedData;
        for (OneHotEncoder encoder : encoders) {
            encodedData = encoder.fit(encodedData).transform(encodedData);
        }
        
        // Combine all feature columns
        List<String> allFeatureCols = new ArrayList<>();
        allFeatureCols.addAll(numericalCols);
        allFeatureCols.addAll(encodedCols);
        
        // Assemble features
        VectorAssembler assembler = new VectorAssembler()
            .setInputCols(allFeatureCols.toArray(new String[0]))
            .setOutputCol("features");
        
        Dataset<Row> featuresData = assembler.transform(encodedData);
        
        // Scale features
        StandardScaler scaler = new StandardScaler()
            .setInputCol("features")
            .setOutputCol("scaledFeatures")
            .setWithStd(true)
            .setWithMean(true);
        
        return scaler.fit(featuresData).transform(featuresData);
    }

    /**
     * Create split ratios for dividing data into batches.
     */
    private double[] createSplitRatios() {
        double[] ratios = new double[numBatches];
        Arrays.fill(ratios, 1.0 / numBatches);
        return ratios;
    }

    /**
     * Write clustering results to CSV file.
     */
    private String writeResultsToFile(Dataset<Row> results) throws IOException {
        String outputFilePath = outputDir + "/" + System.getProperty("user.name") + 
                              "_batch_kmeans_" + datasetName + ".csv";
        
        File outputFile = new File(outputFilePath);
        File outputDirectory = outputFile.getParentFile();
        
        // Create directory if it doesn't exist
        if (outputDirectory != null && !outputDirectory.exists()) {
            outputDirectory.mkdirs();
        }
        
        // Select relevant columns and write to CSV
        results.select("features", "prediction", "batch")
              .write()
              .option("header", "true")
              .csv(outputFilePath);
        
        return outputFilePath;
    }

    /**
     * Class to hold the results of the batch clustering analysis.
     */
    public static class BatchClusteringResult implements Serializable {
        private static final long serialVersionUID = 1L;
        
        private final transient KMeansModel finalModel;
        private final double cost;
        private final String outputFilePath;
        private final List<KMeansModel> batchModels;
        private final int batchesProcessed;

        public BatchClusteringResult(
                KMeansModel finalModel,
                double cost,
                String outputFilePath,
                List<KMeansModel> batchModels,
                int batchesProcessed) {
            this.finalModel = finalModel;
            this.cost = cost;
            this.outputFilePath = outputFilePath;
            this.batchModels = batchModels;
            this.batchesProcessed = batchesProcessed;
        }

        // Getters and toString method
        public KMeansModel getFinalModel() { return finalModel; }
        public double getCost() { return cost; }
        public String getOutputFilePath() { return outputFilePath; }
        public List<KMeansModel> getBatchModels() { return batchModels; }
        public int getBatchesProcessed() { return batchesProcessed; }
        
        @Override
        public String toString() {
            return "BatchClusteringResult{" +
                    "cost=" + cost +
                    ", outputFilePath='" + outputFilePath + '\'' +
                    ", numClusters=" + (finalModel != null ? finalModel.clusterCenters().length : 0) +
                    ", batchesProcessed=" + batchesProcessed +
                    '}';
        }
    }
}
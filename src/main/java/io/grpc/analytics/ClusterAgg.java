package io.grpc.analytics;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * ClusterAgg: A class for aggregating multiple clustered datasets
 * and calculating cluster means for a new dataset.
 */
public class ClusterAgg implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private JavaSparkContext sc;
    private List<JavaRDD<DataPoint>> datasets;
    private JavaRDD<DataPoint> combinedRDD;
    // Here, clusterMeansRDD now holds a single double value representing the mean of all features per cluster.
    private JavaPairRDD<Integer, Double> clusterMeansRDD;
    private JavaRDD<DataPointWithMean> resultRDD;
    
    /**
     * A class to represent a data point with features, cluster id, and source id
     */
    public static class DataPoint implements Serializable {
        private static final long serialVersionUID = 1L;
        
        private double[] features;
        private int clusterId;
        private int sourceId;
        
        public DataPoint(double[] features, int clusterId, int sourceId) {
            this.features = features;
            this.clusterId = clusterId;
            this.sourceId = sourceId;
        }
        
        public double[] getFeatures() {
            return features;
        }
        
        public int getClusterId() {
            return clusterId;
        }
        
        public int getSourceId() {
            return sourceId;
        }
        
        public Vector toVector() {
            return Vectors.dense(features);
        }
        
        @Override
        public String toString() {
            return "DataPoint{" +
                    "features=" + Arrays.toString(features) +
                    ", clusterId=" + clusterId +
                    ", sourceId=" + sourceId +
                    '}';
        }
    }
    
    /**
     * A class to represent a data point with the original data and cluster mean
     */
    public static class DataPointWithMean implements Serializable {
        private static final long serialVersionUID = 1L;
        
        private DataPoint originalPoint;
        private double clusterMean;
        
        public DataPointWithMean(DataPoint originalPoint, double clusterMean) {
            this.originalPoint = originalPoint;
            this.clusterMean = clusterMean;
        }
        
        public DataPoint getOriginalPoint() {
            return originalPoint;
        }
        
        public double getClusterMean() {
            return clusterMean;
        }
        
        @Override
        public String toString() {
            return "DataPointWithMean{" +
                    "originalPoint=" + originalPoint +
                    ", clusterMean=" + clusterMean +
                    '}';
        }
    }
    
    /**
     * Constructor for ClusterAgg
     * 
     * @param sparkContext An existing JavaSparkContext. If null, a new context will be created.
     */
    public ClusterAgg(JavaSparkContext sparkContext) {
        if (sparkContext != null) {
            this.sc = sparkContext;
        } else {
            SparkConf conf = new SparkConf().setAppName("ClusterAgg");
            this.sc = new JavaSparkContext(conf);
        }
        
        this.datasets = new ArrayList<>();
    }
    
    /**
     * Load datasets from CSV files and parse them into DataPoint objects
     * 
     * @param paths List of paths to the datasets
     * @param featureIndices Indices of columns to use as features
     * @param clusterIndex Index of column containing cluster assignments
     * @param hasHeader Boolean indicating if the CSV has a header
     * @return this object for method chaining
     */
    public ClusterAgg loadDatasets(List<String> paths, final int[] featureIndices, 
                                 final int clusterIndex, final boolean hasHeader) {
        for (int sourceId = 0; sourceId < paths.size(); sourceId++) {
            final int currentSourceId = sourceId;
            
            // Read the file as text
            JavaRDD<String> lines = sc.textFile(paths.get(sourceId));
            
          //  Skip header if needed (code commented out; uncomment if required)
            if (hasHeader) {
                JavaPairRDD<String,Long> indexedLines = lines.zipWithIndex();
                lines = indexedLines.filter(new Function<Tuple2<String, Long>, Boolean>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Boolean call(Tuple2<String, Long> t) throws Exception {
                        return t._2() > 0;
                    }
                }).map(new Function<Tuple2<String, Long>, String>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public String call(Tuple2<String, Long> t) throws Exception {
                        return t._1();
                    }
                });
            }
            
            // Parse CSV and create DataPoint objects
            JavaRDD<DataPoint> dataRDD = lines.map(new Function<String, DataPoint>() {
                private static final long serialVersionUID = 1L;
                @Override
                public DataPoint call(String line) throws Exception {
                    String[] parts = line.split(",");
                    double[] features = new double[featureIndices.length];
                    
                    for (int i = 0; i < featureIndices.length; i++) {
                        features[i] = Double.parseDouble(parts[featureIndices[i]]);
                    }
                    
                    int clusterId = Integer.parseInt(parts[clusterIndex]);
                    return new DataPoint(features, clusterId, currentSourceId);
                }
            });
            
            datasets.add(dataRDD);
        }
        
        return this;
    }
    
    /**
     * Combine all datasets into a single RDD
     * 
     * @return this object for method chaining
     */
    public ClusterAgg combineDatasets() {
        if (datasets == null || datasets.isEmpty()) {
            throw new IllegalStateException("No datasets loaded. Call loadDatasets first.");
        }
        
        if (combinedRDD != null) {
            combinedRDD.unpersist();
        }
        JavaRDD<DataPoint>[] datasetArray = datasets.toArray(new JavaRDD[datasets.size()]);
        combinedRDD = sc.union(datasetArray);
        combinedRDD.cache(); // Cache for better performance
        
        return this;
    }
    
    /**
     * Calculate the mean value for each cluster
     * This method uses reduceByKey to aggregate feature vectors and counts for each cluster,
     * then calculates the mean value (averaging across all feature dimensions).
     * 
     * @return this object for method chaining
     */
    public ClusterAgg calculateClusterMeans() {
        if (combinedRDD == null) {
            throw new IllegalStateException("No combined dataset available. Call combineDatasets first.");
        }
        
        // Map each DataPoint to (clusterId, (featureVector, 1))
        JavaPairRDD<Integer, Tuple2<double[], Integer>> clusterFeatureSum = combinedRDD.mapToPair(
            new PairFunction<DataPoint, Integer, Tuple2<double[], Integer>>() {
                private static final long serialVersionUID = 1L;
                @Override
                public Tuple2<Integer, Tuple2<double[], Integer>> call(DataPoint dataPoint) {
                    return new Tuple2<>(dataPoint.getClusterId(), new Tuple2<>(dataPoint.getFeatures(), 1));
                }
            }
        );
        
        // Reduce by key: sum feature vectors and count data points per cluster
        JavaPairRDD<Integer, Tuple2<double[], Integer>> clusterAggregates = clusterFeatureSum.reduceByKey(
            new Function2<Tuple2<double[], Integer>, Tuple2<double[], Integer>, Tuple2<double[], Integer>>() {
                private static final long serialVersionUID = 1L;
                @Override
                public Tuple2<double[], Integer> call(Tuple2<double[], Integer> a, Tuple2<double[], Integer> b) {
                    double[] sumFeatures = new double[a._1.length];
                    for (int i = 0; i < sumFeatures.length; i++) {
                        sumFeatures[i] = a._1[i] + b._1[i];
                    }
                    return new Tuple2<>(sumFeatures, a._2 + b._2);
                }
            }
        );
        
        // Compute mean for each cluster by averaging across all feature dimensions
        clusterMeansRDD = clusterAggregates.mapToPair(
            new PairFunction<Tuple2<Integer, Tuple2<double[], Integer>>, Integer, Double>() {
                private static final long serialVersionUID = 1L;
                @Override
                public Tuple2<Integer, Double> call(Tuple2<Integer, Tuple2<double[], Integer>> clusterData) {
                    Integer clusterId = clusterData._1();
                    double[] sumFeatures = clusterData._2()._1();
                    int count = clusterData._2()._2();
                    
                    double mean = 0.0;
                    for (double feature : sumFeatures) {
                        mean += feature;
                    }
                    // Average over the number of features
                    mean /= (sumFeatures.length * count);
                    return new Tuple2<>(clusterId, mean);
                }
            }
        );
        
        clusterMeansRDD.cache();
        return this;
    }
    
    /**
     * Create a new dataset that includes the original data points and their cluster means
     * 
     * @return this object for method chaining
     */
    public ClusterAgg createEnrichedDataset() {
        if (combinedRDD == null || clusterMeansRDD == null) {
            throw new IllegalStateException("Combined dataset or cluster means not available. Call calculateClusterMeans first.");
        }
        
        // Collect cluster means to a map (this is efficient if number of clusters is small)
        final Map<Integer, Double> clusterMeansMap = clusterMeansRDD.collectAsMap();
        
        // Join data points with their cluster means
        resultRDD = combinedRDD.map(
            new Function<DataPoint, DataPointWithMean>() {
                private static final long serialVersionUID = 1L;
                @Override
                public DataPointWithMean call(DataPoint dataPoint) throws Exception {
                    int clusterId = dataPoint.getClusterId();
                    Double mean = clusterMeansMap.getOrDefault(clusterId, 0.0);
                    return new DataPointWithMean(dataPoint, mean);
                }
            }
        );
        
        resultRDD.cache();
        return this;
    }
    
    /**
     * Save the results to a CSV file
     * 
     * @param outputPath Path to save the results
     */
    public void saveResults(String outputPath) {
        if (resultRDD == null) {
            throw new IllegalStateException("No result dataset available. Call createEnrichedDataset first.");
        }
        
        // Convert to string format for saving
        JavaRDD<String> outputRDD = resultRDD.map(
            new Function<DataPointWithMean, String>() {
                private static final long serialVersionUID = 1L;
                @Override
                public String call(DataPointWithMean dataPointWithMean) throws Exception {
                    DataPoint dp = dataPointWithMean.getOriginalPoint();
                    double mean = dataPointWithMean.getClusterMean();
                    
                    StringBuilder sb = new StringBuilder();
                    
                    // Add original features
                    for (double feature : dp.getFeatures()) {
                        sb.append(feature).append(",");
                    }
                    
                    // Add original cluster, source ID and cluster mean
                    sb.append(dp.getClusterId()).append(",")
                      .append(dp.getSourceId()).append(",")
                      .append(mean);
                    
                    return sb.toString();
                }
            }
        );
        
        // Get sample data point to determine feature count
        DataPointWithMean samplePoint = resultRDD.first();
        final String header = createHeader(samplePoint.getOriginalPoint().getFeatures().length);
        
        // More efficient way to add header using coalesce and mapPartitionsWithIndex
        JavaRDD<String> finalOutput = outputRDD.coalesce(1).mapPartitionsWithIndex(
            new Function2<Integer, java.util.Iterator<String>, java.util.Iterator<String>>() {
                private static final long serialVersionUID = 1L;
                @Override
                public java.util.Iterator<String> call(Integer index, java.util.Iterator<String> iterator) throws Exception {
                    List<String> result = new ArrayList<>();
                    if (index == 0) {
                        result.add(header);
                    }
                    while (iterator.hasNext()) {
                        result.add(iterator.next());
                    }
                    return result.iterator();
                }
            }, true);
        
        // Save to disk
        finalOutput.saveAsTextFile(outputPath);
    }
    
    /**
     * Create a header string for the output CSV
     * 
     * @param numFeatures Number of features in the dataset
     * @return Header string
     */
    private String createHeader(int numFeatures) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numFeatures; i++) {
            sb.append("feature").append(i).append(",");
        }
        sb.append("originalCluster,sourceId,clusterMean");
        return sb.toString();
    }
    
    /**
     * Get the cluster means RDD
     * 
     * @return RDD of cluster IDs and their corresponding mean values
     */
    public JavaPairRDD<Integer, Double> getClusterMeansRDD() {
        return clusterMeansRDD;
    }
    
    /**
     * Get the result RDD containing original data points and their cluster means
     * 
     * @return RDD of DataPointWithMean objects
     */
    public JavaRDD<DataPointWithMean> getResultRDD() {
        return resultRDD;
    }
    
    /**
     * Close the Spark context
     */
    public void close() {
        // Unpersist cached RDDs before closing
        if (combinedRDD != null) combinedRDD.unpersist();
        if (clusterMeansRDD != null) clusterMeansRDD.unpersist();
        if (resultRDD != null) resultRDD.unpersist();
        
        sc.close();
    }
    
    /**
     * Example usage
     */
    public static void main(String[] args) {
        // Create a Spark context
        SparkConf conf = new SparkConf().setAppName("ClusterMeanExample").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        try {
            // Define input datasets and output path
            List<String> inputPaths = Arrays.asList("complete_cluster_assignments.csv", "complete_cluster_assignments.csv");
            String outputPath = "enriched_dataset";
            
            // Define feature indices and cluster index
            int[] featureIndices = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};  // Columns for features
            int clusterIndex = 13;  // Column for cluster ID
            boolean hasHeader = false;
            
            // Create instance and process
            ClusterAgg clusterAgg = new ClusterAgg(sc)
                .loadDatasets(inputPaths, featureIndices, clusterIndex, hasHeader)
                .combineDatasets()
                .calculateClusterMeans()
                .createEnrichedDataset();
            
            // Save results
            clusterAgg.saveResults(outputPath);
            
            // Print some statistics
            System.out.println("Cluster Means:");
            for (Tuple2<Integer, Double> mean : clusterAgg.getClusterMeansRDD().collect()) {
                System.out.println("Cluster " + mean._1() + ": " + mean._2());
            }
            
            System.out.println("\nProcessed " + clusterAgg.getResultRDD().count() + " data points.");
            
        } finally {
            sc.close();
        }
    }
}

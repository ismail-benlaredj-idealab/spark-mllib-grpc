package io.grpc.analytics;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Simplified ClusterAgg: A class for aggregating multiple clustering results
 */
public class ClusterAgg_V0 implements Serializable {
    private static final long serialVersionUID = 1L;

    private transient JavaSparkContext sc;
    private final List<JavaRDD<DataPoint>> clusteringResults = new ArrayList<>();
    private JavaRDD<DataPoint> combinedRDD;

    public static class DataPoint implements Serializable {
        private static final long serialVersionUID = 1L;
        private final double[] features;
        private final int clusterId;
        private final int clusteringResultId;
        private final int originalRowIndex;

        public DataPoint(double[] features, int clusterId, int clusteringResultId, int originalRowIndex) {
            this.features = features;
            this.clusterId = clusterId;
            this.clusteringResultId = clusteringResultId;
            this.originalRowIndex = originalRowIndex;
        }
        
        public double[] getFeatures() { return features; }
        public int getClusterId() { return clusterId; }
        public int getClusteringResultId() { return clusteringResultId; }
        public int getOriginalRowIndex() { return originalRowIndex; }
    }

    public static class FinalDataPoint implements Serializable {
        private static final long serialVersionUID = 1L;
        private final double[] features;
        private final long consensusClusterId;

        public FinalDataPoint(double[] features, long consensusClusterId) {
            this.features = features;
            this.consensusClusterId = consensusClusterId;
        }
        
        public double[] getFeatures() { return features; }
        public long getConsensusClusterId() { return consensusClusterId; }

        @Override
        public String toString() {
            StringJoiner sj = new StringJoiner(",");
            for (double feature : features) {
                sj.add(String.valueOf(feature));
            }
            sj.add(String.valueOf(consensusClusterId));
            return sj.toString();
        }
    }

    private static class CSVParser implements Function<String, DataPoint> {
        private static final long serialVersionUID = 1L;
        private final int clusteringResultId;
        private int rowCounter = 0;

        public CSVParser(int clusteringResultId) { 
            this.clusteringResultId = clusteringResultId; 
        }

        @Override
        public DataPoint call(String line) {
            String[] parts = line.split(",");
            double[] features = new double[parts.length - 1];
            
            for (int i = 0; i < features.length; i++) {
                try {
                    features[i] = Double.parseDouble(parts[i].trim());
                } catch (NumberFormatException e) {
                    features[i] = 0.0;
                }
            }
            
            int clusterId;
            try {
                clusterId = Integer.parseInt(parts[parts.length - 1].trim());
            } catch (NumberFormatException e) {
                clusterId = parts[parts.length - 1].hashCode();
            }
            
            return new DataPoint(features, clusterId, clusteringResultId, rowCounter++);
        }
    }

    public ClusterAgg_V0(JavaSparkContext sparkContext) {
        this.sc = sparkContext;
    }

    public ClusterAgg_V0 loadClusteringResults(List<String> paths, final boolean hasHeader) {
        for (int i = 0; i < paths.size(); i++) {
            JavaRDD<String> lines = sc.textFile(paths.get(i));
            JavaRDD<String> dataLines = hasHeader ? 
                lines.filter(line -> !line.startsWith("feature") && !line.isEmpty()) : lines;
            clusteringResults.add(dataLines.map(new CSVParser(i)));
        }
        return this;
    }

    public void generateAndSaveConsensusClusters(String outputPath, int numClusters) {
        if (clusteringResults.isEmpty()) {
            throw new IllegalStateException("No clustering results loaded.");
        }
        
        // Combine all clustering results
        JavaRDD<DataPoint>[] resultArray = clusteringResults.toArray(new JavaRDD[0]);
        combinedRDD = sc.union(resultArray).cache();
        
        // Create feature vectors with cluster assignments as additional features
        JavaPairRDD<Integer, double[]> dataWithFeatures = combinedRDD.mapToPair(
            new PairFunction<DataPoint, Integer, double[]>() {
                @Override
                public Tuple2<Integer, double[]> call(DataPoint dp) throws Exception {
                    // Create extended feature vector: original features + cluster assignment as one-hot encoding
                    double[] extendedFeatures = Arrays.copyOf(dp.getFeatures(), dp.getFeatures().length + 1);
                    extendedFeatures[dp.getFeatures().length] = dp.getClusterId();
                    return new Tuple2<>(dp.getOriginalRowIndex(), extendedFeatures);
                }
            }
        );
        
        // Reduce by key to get all cluster assignments for each data point
        JavaPairRDD<Integer, Iterable<double[]>> groupedData = dataWithFeatures.groupByKey();
        
        // For each data point, create a consensus feature vector
        JavaRDD<double[]> consensusFeatures = groupedData.map(tuple -> {
            Iterator<double[]> assignments = tuple._2.iterator();
            double[] first = assignments.next();
            double[] result = Arrays.copyOf(first, first.length);
            int count = 1;
            
            while (assignments.hasNext()) {
                double[] next = assignments.next();
                for (int i = 0; i < result.length; i++) {
                    result[i] += next[i];
                }
                count++;
            }
            
            // Average the features
            for (int i = 0; i < result.length; i++) {
                result[i] /= count;
            }
            
            return result;
        });
        
        // Perform K-means clustering on the consensus features
        org.apache.spark.mllib.clustering.KMeans kmeans = 
            new org.apache.spark.mllib.clustering.KMeans();
        kmeans.setK(numClusters);
        kmeans.setMaxIterations(20);
        
        List<org.apache.spark.mllib.linalg.Vector> vectorList = consensusFeatures.map(
            features -> org.apache.spark.mllib.linalg.Vectors.dense(features)
        ).collect();
        org.apache.spark.mllib.linalg.Vector[] vectors = vectorList.toArray(new org.apache.spark.mllib.linalg.Vector[0]);
        
        org.apache.spark.mllib.clustering.KMeansModel model = kmeans.run(
            sc.parallelize(Arrays.asList(vectors)).rdd()
        );
        
        // Assign consensus clusters
        JavaRDD<FinalDataPoint> finalResult = consensusFeatures.map(features -> {
            int cluster = model.predict(org.apache.spark.mllib.linalg.Vectors.dense(features));
            // Extract original features (remove the cluster assignment part)
            double[] originalFeatures = Arrays.copyOf(features, features.length - 1);
            return new FinalDataPoint(originalFeatures, cluster);
        });
        
        saveFinalCsv(finalResult, outputPath);
    }

    private void saveFinalCsv(JavaRDD<FinalDataPoint> resultRDD, String outputPath) {
        JavaRDD<String> outputRDD = resultRDD.map(FinalDataPoint::toString);
        
        // Create header
        int numFeatures = resultRDD.first().getFeatures().length;
        StringJoiner headerJoiner = new StringJoiner(",");
        for (int i = 0; i < numFeatures; i++) {
            headerJoiner.add("feature_" + i);
        }
        headerJoiner.add("consensus_cluster_id");
        final String header = headerJoiner.toString();
        
        // Add header to output
        JavaRDD<String> finalOutput = outputRDD.coalesce(1).mapPartitionsWithIndex((index, iterator) -> {
            List<String> result = new ArrayList<>();
            if (index == 0) result.add(header);
            iterator.forEachRemaining(result::add);
            return result.iterator();
        }, true);
        
        // Save to file
        Path path = Paths.get(outputPath);
        try {
            if (Files.exists(path)) {
                FileUtils.deleteDirectory(path.toFile());
            }
        } catch (IOException e) {
            System.err.println("Failed to delete existing output directory: " + e.getMessage());
        }
        
        finalOutput.saveAsTextFile(outputPath);
        System.out.println("Consensus clustering results saved to: " + outputPath);
    }

    public void close() {
        if (combinedRDD != null) combinedRDD.unpersist();
    }

    // public static void main(String[] args) {
    //     JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("SimpleClusterAgg").setMaster("local[*]"));
        
    //     SimpleClusterAgg clusterAgg = new SimpleClusterAgg(sc);
        
    //     List<String> clusteringResultPaths = Arrays.asList(
    //         "path/to/dataset_kmeans_clusters.csv",
    //         "path/to/dataset_hierarchical_clusters.csv", 
    //         "path/to/dataset_dbscan_clusters.csv"
    //     );
        
    //     clusterAgg.loadClusteringResults(clusteringResultPaths, true)
    //              .generateAndSaveConsensusClusters("output/consensus_clustering_results", 5);
        
    //     clusterAgg.close();
    //     sc.close();
    // }


    // previously how i use it
        // JavaSparkContext sc = new JavaSparkContext(
            // new SparkConf().setAppName("SimpleClusterAgg").setMaster("local[*]"));
            // ClusterAgg_V0 aggregator = new ClusterAgg_V0(sc); // Pass null to create a
            // new SparkContext
            // List<String> inputPaths = Arrays.asList(
            // "/home/ismail/grpc-java-examples-master/received_files/ismail_kmeans_bank_500_copy.csv",
            // "/home/ismail/grpc-java-examples-master/received_files/ismail_kmeans_bank_500.csv");
            // try {
            // aggregator.loadClusteringResults(inputPaths, true) // true if files have
            // headers
            // .generateAndSaveConsensusClusters("/home/ismail/grpc-java-examples-master/clusterComb",
            // 5);
            // } finally {
            // aggregator.close();
            // }
}
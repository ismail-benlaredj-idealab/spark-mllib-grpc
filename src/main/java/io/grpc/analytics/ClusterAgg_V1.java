package io.grpc.analytics;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.Serializable;
import java.util.*;

public class ClusterAgg_V1 implements Serializable {

    private static final long serialVersionUID = 1L;

    private final transient JavaSparkContext jsc;
    private final List<String> datasetPaths;
    private final String outputFilePath;
    private final int numClusters;
    private final int numIterations;

    public ClusterAgg_V1(JavaSparkContext jsc,
                         List<String> datasetPaths,
                         String outputFilePath,
                         int numClusters,
                         int numIterations) {
        this.jsc = jsc;
        this.datasetPaths = datasetPaths;
        this.outputFilePath = outputFilePath;
        this.numClusters = numClusters;
        this.numIterations = numIterations;
    }

    private static String[] parseCSVLine(String line) {
        return line.split(",");
    }

    public void runClustering() throws Exception {
        int numRows = -1;
        List<double[]> lastColumns = new ArrayList<>();
        List<String[]> originalRows = null;
        String[] header = null;

        // Step 1: Read all datasets, store last column, and keep one dataset as reference
        for (String path : datasetPaths) {
            JavaRDD<String> data = jsc.textFile(path);
            String datasetHeader = data.first();

            List<String[]> rows = data.filter(line -> !line.equals(datasetHeader))
                                      .map(ClusterAgg_V1::parseCSVLine)
                                      .collect();

            if (numRows == -1) numRows = rows.size();
            else if (rows.size() != numRows) {
                throw new RuntimeException("All datasets must have the same number of rows!");
            }

            // Keep the first dataset as reference for original rows and header
            if (originalRows == null) {
                originalRows = rows;
                header = datasetHeader.split(",");
            }

            double[] lastCol = new double[rows.size()];
            for (int i = 0; i < rows.size(); i++) {
                lastCol[i] = Double.parseDouble(rows.get(i)[rows.get(i).length - 1].trim());
            }
            lastColumns.add(lastCol);
        }

        // Step 2: Build table of last columns (rows become vectors)
        List<Vector> rowVectors = new ArrayList<>();
        for (int i = 0; i < numRows; i++) {
            double[] rowVector = new double[lastColumns.size()];
            for (int j = 0; j < lastColumns.size(); j++) {
                rowVector[j] = lastColumns.get(j)[i];
            }
            rowVectors.add(Vectors.dense(rowVector));
        }

        JavaRDD<Vector> parsedData = jsc.parallelize(rowVectors);

        // Step 3: Cluster rows
        KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);

        List<Integer> rowClusterAssignments = new ArrayList<>();
        for (Vector v : rowVectors) {
            rowClusterAssignments.add(clusters.predict(v));
        }

        // Step 4: Write output dataset with clusterOfClusters column
        File outFile = new File(outputFilePath);
        if (outFile.getParentFile() != null && !outFile.getParentFile().exists()) {
            outFile.getParentFile().mkdirs();
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath))) {
            // Write header
            for (String h : header) writer.write(h + ",");
            writer.write("clusterOfClusters\n");

            // Write rows with cluster assignment
            for (int i = 0; i < numRows; i++) {
                for (String val : originalRows.get(i)) {
                    writer.write(val + ",");
                }
                writer.write(rowClusterAssignments.get(i) + "\n");
            }
        }

        System.out.println("Clustering finished. Output saved to: " + outputFilePath);
    }
}

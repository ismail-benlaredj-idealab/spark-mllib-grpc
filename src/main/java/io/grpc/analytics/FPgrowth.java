package io.grpc.analytics;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;

import java.util.ArrayList;
import java.util.List;
import java.io.Serializable;
import java.nio.file.Paths;

public class FPgrowth implements Serializable {
    private static final long serialVersionUID = 1L;

    // Configuration parameters
    private SparkConf conf;
    private String datasetPath;
    private double minSupport;
    private int numPartitions;
    private String outputPath;
    private String datasetName;

    /**
     * Fully configurable constructor
     * 
     * @param datasetPath   Path to the input CSV file
     * @param minSupport    Minimum support threshold for frequent itemsets
     * @param numPartitions Number of partitions for distributed processing
     * @param outputPath    Path to save frequent itemsets
     */
    public FPgrowth(
            SparkConf conf,
            String datasetPath,
            double minSupport,
            int numPartitions,
            String outputPath,
            String datasetName) {
        this.conf = conf;
        this.datasetPath = datasetPath;
        this.minSupport = minSupport;
        this.numPartitions = numPartitions;
        this.outputPath = Paths.get(outputPath+"/"+System.getProperty("user.name")+"_"+datasetName.split("\\.")[0]+"_FP_Growth.dat").toAbsolutePath().toString();
    }

    /**
     * Perform FP-Growth analysis
     * 
     * @return List of frequent itemsets
     */
    public List<FPGrowth.FreqItemset<String>> analyze() {

        // Create Spark Context
        JavaSparkContext sc = new JavaSparkContext(conf);
        try {
            // Read the dataset
            JavaRDD<String> data = sc.textFile(datasetPath);

            // Get header
            String header = data.first();
            String[] columnNames = header.split(",");

            // Skip header and process data
            JavaRDD<List<String>> transactions = data
                    .filter(line -> !line.equals(header))
                    .map(line -> {
                        // Split the line into values
                        String[] values = line.split(",");

                        // Create unique items by combining column names and values
                        List<String> uniqueItems = new ArrayList<>();
                        for (int i = 0; i < Math.min(values.length, columnNames.length); i++) {
                            // Skip empty or null values
                            if (values[i] != null && !values[i].trim().isEmpty()) {
                                // Create unique item by combining column name and value
                                String uniqueItem = columnNames[i] + "_" + values[i].trim();
                                uniqueItems.add(uniqueItem);
                            }
                        }

                        return uniqueItems;
                    });
            // Create FP-Growth algorithm
            FPGrowth fpGrowth = new FPGrowth()
                    .setMinSupport(minSupport)
                    .setNumPartitions(numPartitions);

            // Train the FP-Growth model
            FPGrowthModel<String> model = fpGrowth.run(transactions);

            // Collect frequent itemsets
            List<FPGrowth.FreqItemset<String>> freqItemsets = model.freqItemsets().toJavaRDD().collect();

            // Save frequent itemsets
            saveFrequentItemsets(freqItemsets);

            return freqItemsets;

        } catch (Exception e) {
            System.err.println("Error processing dataset: " + e.getMessage());
            e.printStackTrace();
            return new ArrayList<>();
        } finally {
            // Stop Spark context
            sc.stop();
        }
    }

    /**
     * Save frequent itemsets to a text file
     * 
     * @param freqItemsets List of frequent itemsets to save
     */
    private void saveFrequentItemsets(List<FPGrowth.FreqItemset<String>> freqItemsets) {
        // Create a list to store formatted itemsets
        List<String> formattedItemsets = new ArrayList<>();

        // Add header
        formattedItemsets.add("Itemset,Frequency");
        // Format each frequent itemset
        for (FPGrowth.FreqItemset<String> itemset : freqItemsets) {
            // Convert itemset to comma-separated string
            String itemsetStr = String.join(";", itemset.javaItems());

            // Add frequency
            String line = itemsetStr + "," + itemset.freq();
            formattedItemsets.add(line);
        }
        // Write to file
        try {
            java.nio.file.Files.write(
                    java.nio.file.Paths.get(outputPath),
                    formattedItemsets,
                    java.nio.charset.StandardCharsets.UTF_8);
            System.out.println("Frequent itemsets saved to " + outputPath);
        } catch (Exception e) {
            System.err.println("Error saving frequent itemsets: " + e.getMessage());
        }
    }

}
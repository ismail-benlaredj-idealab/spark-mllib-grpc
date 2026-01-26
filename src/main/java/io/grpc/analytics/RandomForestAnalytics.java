package io.grpc.analytics;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.*;
import org.apache.spark.ml.regression.RandomForestRegressionModel;
import org.apache.spark.ml.regression.RandomForestRegressor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RandomForestAnalytics {
    private SparkSession spark;
    private String datasetPath;
    private String outputDir = "node1";

    /**
     * Constructor for RandomForestAnalytics
     * 
     * @param sparkSession An existing SparkSession
     * @param datasetPath  Path to the CSV dataset
     */
    public RandomForestAnalytics(SparkSession sparkSession, String datasetPath) {
        this.spark = sparkSession;
        this.datasetPath = datasetPath;
    }

    /**
     * Constructor with output directory specification
     * 
     * @param sparkSession An existing SparkSession
     * @param datasetPath  Path to the CSV dataset
     * @param outputDir    Directory to save results
     */
    public RandomForestAnalytics(SparkSession sparkSession, String datasetPath, String outputDir) {
        this.spark = sparkSession;
        this.datasetPath = datasetPath;
        this.outputDir = outputDir;
    }

    /**
     * Main method to run the random forest analysis
     * 
     * @return PipelineModel The trained model
     */
    public PipelineModel runAnalysis() {
        // Load dataset with header to infer schema automatically
        Dataset<Row> data = spark.read()
                .option("header", "true") // Use first row as header
                .option("inferSchema", "true") // Automatically infer column types
                .option("delimiter", ",") // Define delimiter (comma for CSV)
                .csv(datasetPath);

        // Drop the first column (index 0) after loading the dataset
        String firstColumn = data.columns()[0]; // Get the name of the first column
        data = data.drop(firstColumn); // Drop the first column

        // Display detected schema
        System.out.println("Detected Schema:");
        data.printSchema();

        // Display sample data
        System.out.println("Sample data:");
        data.show(5);

        // Get column names and determine feature types
        String[] allColumns = data.columns();
        String labelColumn = allColumns[allColumns.length - 1]; // Last column is label

        // Create feature columns array (all columns except label)
        String[] featureColumns = Arrays.copyOfRange(allColumns, 0, allColumns.length - 1);

        // Separate numerical and categorical features
        List<String> numericFeaturesList = new ArrayList<>();
        List<String> categoricalFeaturesList = new ArrayList<>();

        StructField[] fields = data.schema().fields();
        for (int i = 0; i < featureColumns.length; i++) {
            DataType dataType = fields[i].dataType();
            if (dataType == DataTypes.StringType) {
                categoricalFeaturesList.add(featureColumns[i]);
            } else {
                // Assuming numeric types (double, integer, etc.)
                numericFeaturesList.add(featureColumns[i]);
            }
        }

        String[] numericFeatures = numericFeaturesList.toArray(new String[0]);
        String[] categoricalFeatures = categoricalFeaturesList.toArray(new String[0]);

        System.out.println("Detected label column: " + labelColumn);
        System.out.println("Detected numeric features: " + Arrays.toString(numericFeatures));
        System.out.println("Detected categorical features: " + Arrays.toString(categoricalFeatures));

        // Pipeline stages
        List<PipelineStage> pipelineStages = new ArrayList<>();

        // Create a list to hold all feature columns for the final vector assembler
        List<String> allFeatureColumns = new ArrayList<>(numericFeaturesList);

        // Add an Imputer stage before the VectorAssembler to replace NaN values
        Imputer imputer = new Imputer()
                .setInputCols(numericFeatures)
                .setOutputCols(Arrays.stream(numericFeatures).map(c -> c + "Imputed").toArray(String[]::new))
                .setStrategy("median"); // You can use "mean", "median", or a specific value

        pipelineStages.add(imputer);

        // Update the allFeatureColumns list to use the imputed columns instead of
        // original ones
        for (int i = 0; i < numericFeatures.length; i++) {
            // Remove the original numeric feature and add the imputed version
            allFeatureColumns.remove(numericFeatures[i]);
            allFeatureColumns.add(numericFeatures[i] + "Imputed");
        }

        // Process categorical features - using string indexer only
        for (String categoricalFeature : categoricalFeatures) {
            String indexedFeature = categoricalFeature + "Indexed";

            StringIndexer indexer = new StringIndexer()
                    .setInputCol(categoricalFeature)
                    .setOutputCol(indexedFeature)
                    .setHandleInvalid("keep");

            pipelineStages.add(indexer);
            allFeatureColumns.add(indexedFeature);
        }

        // Create feature vector using all processed features
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(allFeatureColumns.toArray(new String[0]))
                .setOutputCol("features")
                .setHandleInvalid("keep"); // Handle invalid data

        pipelineStages.add(assembler);

        // split the data to training and testing sets
        Dataset<Row>[] splits = data.randomSplit(new double[] { 0.8, 0.2 }, 1234);
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];

        int requiredMaxBins = 32; // Default
        if (categoricalFeatures.length > 0) {
            int maxCategories = 0;
            for (String feature : categoricalFeatures) {
                long distinctCount = data.select(feature).distinct().count();
                if (distinctCount > maxCategories) {
                    maxCategories = (int) distinctCount;
                }
            }
            requiredMaxBins = Math.max(32, maxCategories + 10);
            System.out.println("Detected maximum categories: " + maxCategories);
            System.out.println("Setting maxBins to: " + requiredMaxBins);
        }

        VectorIndexer vectorIndexer = new VectorIndexer()
                .setInputCol("features")
                .setOutputCol("indexedFeatures")
                .setMaxCategories(requiredMaxBins)
                .setHandleInvalid("keep");

        pipelineStages.add(vectorIndexer);

        // Update RandomForestRegressor
        RandomForestRegressor rf = new RandomForestRegressor()
                .setLabelCol(labelColumn)
                .setFeaturesCol("indexedFeatures")
                .setNumTrees(10)
                .setMaxDepth(5)
                .setSeed(1234)
                .setMaxBins(requiredMaxBins);

        pipelineStages.add(rf);

        // Create and run the pipeline
        Pipeline pipeline = new Pipeline().setStages(pipelineStages.toArray(new PipelineStage[0]));
        PipelineModel model = pipeline.fit(trainingData);

        // Extract the RF model from the pipeline
        RandomForestRegressionModel rfModel = (RandomForestRegressionModel) model.stages()[model.stages().length - 1];

        // Save the tree information to files
        saveTreesInformation(rfModel, featureColumns);

        // Make predictions
        Dataset<Row> predictions = model.transform(testData);

        // Select predicted and actual values
        Dataset<Row> predictionsAndLabels = predictions.select("prediction", labelColumn, "features");
        System.out.println("Predictions and Labels:");
        predictionsAndLabels.show(10);

        // Save predictions to CSV
        try {
            predictionsAndLabels
                    .coalesce(1)
                    .write()
                    .option("header", "true")
                    .mode("overwrite")
                    .csv(outputDir + "/predictions");
            System.out.println("Saved predictions to " + outputDir + "/predictions directory");
        } catch (Exception e) {
            System.err.println("Error saving predictions: " + e.getMessage());
        }

        // Evaluate the model
        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setLabelCol(labelColumn)
                .setPredictionCol("prediction")
                .setMetricName("rmse");

        double rmse = evaluator.evaluate(predictions);
        System.out.println("Root Mean Squared Error (RMSE) = " + rmse);

        evaluator.setMetricName("r2");
        double r2 = evaluator.evaluate(predictions);
        System.out.println("R² = " + r2);

        evaluator.setMetricName("mae");
        double mae = evaluator.evaluate(predictions);
        System.out.println("Mean Absolute Error (MAE) = " + mae);

        // Save model performance metrics
        saveModelPerformance(predictions, evaluator, labelColumn);

        return model;
    }

    private void saveTreesInformation(RandomForestRegressionModel model, String[] featureColumns) {
        try {
            // Create directory if it doesn't exist
            Files.createDirectories(Paths.get(outputDir + "/trees"));

            // Save the feature importances
            StringBuilder featureImportances = new StringBuilder();
            featureImportances.append("Feature Importances:\n");
            double[] importances = model.featureImportances().toArray();

            // Map importance values to original feature names where possible
            for (int i = 0; i < importances.length; i++) {
                String featureName = (i < featureColumns.length) ? featureColumns[i] : "Feature " + i;
                featureImportances.append(featureName).append(": ").append(importances[i]).append("\n");
            }
            Files.write(Paths.get(outputDir + "/feature_importances.txt"), featureImportances.toString().getBytes());

            // Save each tree's information
            for (int i = 0; i < model.getNumTrees(); i++) {
                String treeInfo = model.trees()[i].toDebugString();
                Files.write(Paths.get(outputDir + "/trees/tree_" + i + ".txt"), treeInfo.getBytes());
            }

            // Save overall model information
            String modelInfo = model.toDebugString();
            Files.write(Paths.get(outputDir + "/model_info.txt"), modelInfo.getBytes());

            System.out.println("Saved decision trees information to " + outputDir + "/trees directory");
        } catch (IOException e) {
            System.err.println("Error saving tree information: " + e.getMessage());
        }
    }

    private void saveModelPerformance(Dataset<Row> predictions, RegressionEvaluator evaluator,
            String labelColumn) {
        try {
            // Create directory if it doesn't exist
            Files.createDirectories(Paths.get(outputDir));

            StringBuilder performanceMetrics = new StringBuilder();

            // Calculate various metrics
            evaluator.setMetricName("rmse");
            double rmse = evaluator.evaluate(predictions);
            performanceMetrics.append("Root Mean Squared Error (RMSE): ").append(rmse).append("\n");

            evaluator.setMetricName("mse");
            double mse = evaluator.evaluate(predictions);
            performanceMetrics.append("Mean Squared Error (MSE): ").append(mse).append("\n");

            evaluator.setMetricName("r2");
            double r2 = evaluator.evaluate(predictions);
            performanceMetrics.append("R²: ").append(r2).append("\n");

            evaluator.setMetricName("mae");
            double mae = evaluator.evaluate(predictions);
            performanceMetrics.append("Mean Absolute Error (MAE): ").append(mae).append("\n");

            // Calculate prediction statistics
            Row predictionStatsRow = predictions.select(
                    org.apache.spark.sql.functions.min("prediction"),
                    org.apache.spark.sql.functions.max("prediction"),
                    org.apache.spark.sql.functions.avg("prediction"),
                    org.apache.spark.sql.functions.stddev("prediction")).first();

            performanceMetrics.append("\nPrediction Statistics:\n");
            performanceMetrics.append("Min: ").append(predictionStatsRow.getDouble(0)).append("\n");
            performanceMetrics.append("Max: ").append(predictionStatsRow.getDouble(1)).append("\n");
            performanceMetrics.append("Mean: ").append(predictionStatsRow.getDouble(2)).append("\n");
            performanceMetrics.append("StdDev: ").append(predictionStatsRow.getDouble(3)).append("\n");

            // Calculate actual value statistics
            Row actualStatsRow = predictions.select(
                    org.apache.spark.sql.functions.min(labelColumn),
                    org.apache.spark.sql.functions.max(labelColumn),
                    org.apache.spark.sql.functions.avg(labelColumn),
                    org.apache.spark.sql.functions.stddev(labelColumn)).first();

            performanceMetrics.append("\nActual Value Statistics:\n");
            performanceMetrics.append("Min: ").append(actualStatsRow.getDouble(0)).append("\n");
            performanceMetrics.append("Max: ").append(actualStatsRow.getDouble(1)).append("\n");
            performanceMetrics.append("Mean: ").append(actualStatsRow.getDouble(2)).append("\n");
            performanceMetrics.append("StdDev: ").append(actualStatsRow.getDouble(3)).append("\n");

            // Calculate error statistics
            Dataset<Row> errorData = predictions.withColumn(
                    "error",
                    org.apache.spark.sql.functions.col("prediction")
                            .minus(org.apache.spark.sql.functions.col(labelColumn)));

            Row errorStatsRow = errorData.select(
                    org.apache.spark.sql.functions.min("error"),
                    org.apache.spark.sql.functions.max("error"),
                    org.apache.spark.sql.functions.avg("error"),
                    org.apache.spark.sql.functions.stddev("error"),
                    org.apache.spark.sql.functions.abs(org.apache.spark.sql.functions.avg("error"))
                            .as("abs_mean_error"))
                    .first();

            performanceMetrics.append("\nError Statistics:\n");
            performanceMetrics.append("Min Error: ").append(errorStatsRow.getDouble(0)).append("\n");
            performanceMetrics.append("Max Error: ").append(errorStatsRow.getDouble(1)).append("\n");
            performanceMetrics.append("Mean Error: ").append(errorStatsRow.getDouble(2)).append("\n");
            performanceMetrics.append("StdDev Error: ").append(errorStatsRow.getDouble(3)).append("\n");
            performanceMetrics.append("Absolute Mean Error: ").append(errorStatsRow.getDouble(4)).append("\n");

            // Save all performance metrics to a file
            Files.write(Paths.get(outputDir + "/model_performance.txt"), performanceMetrics.toString().getBytes());
            System.out.println("Saved model performance metrics to " + outputDir + "/model_performance.txt");

        } catch (IOException e) {
            System.err.println("Error saving model performance: " + e.getMessage());
        }
    }

}
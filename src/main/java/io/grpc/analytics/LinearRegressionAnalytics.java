package io.grpc.analytics;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LinearRegressionAnalytics {

        private final SparkSession sparkSession;
        private final String datasetPath;
        private final String outputDir;

        /**
         * Constructor for the LinearRegressionAnalytics class.
         * 
         * @param sparkSession The SparkSession to use for the analysis
         * @param datasetPath  The path to the input dataset
         * @param outputDir    The directory to save the output results
         */
        public LinearRegressionAnalytics(SparkSession sparkSession, String datasetPath, String outputDir) {
                this.sparkSession = sparkSession;
                this.datasetPath = datasetPath;
                this.outputDir = outputDir;
        }

        /**
         * Run the linear regression analysis on the provided dataset.
         * 
         * @return A summary of the analysis results
         * @throws Exception If an error occurs during analysis
         */
        public LinearRegressionResult runAnalysis() throws Exception {
                try {
                        // Load the dataset
                        Dataset<Row> data = sparkSession.read()
                                        .option("header", "true")
                                        .option("inferSchema", "true")
                                        .csv(datasetPath);

                        // Print dataset information
                        System.out.println("Dataset loaded with " + data.count() + " rows and " + data.columns().length
                                        + " columns");
                        System.out.println("Schema:");
                        data.printSchema();
                        data.show(5);

                        // Identify categorical and numerical columns
                        List<String> categoricalCols = new ArrayList<>();
                        List<String> numericCols = new ArrayList<>();
                        String labelColumn = null;

                        // Assume the last column is the target variable
                        StructField[] fields = data.schema().fields();
                        for (int i = 0; i < fields.length; i++) {
                                StructField field = fields[i];
                                String fieldName = field.name();

                                if (i == fields.length - 1) {
                                        // Assume the last column is the label
                                        labelColumn = fieldName;
                                        // Check if label is numeric
                                        if (!field.dataType().equals(DataTypes.DoubleType) &&
                                                        !field.dataType().equals(DataTypes.IntegerType) &&
                                                        !field.dataType().equals(DataTypes.FloatType)) {
                                                throw new IllegalArgumentException(
                                                                "Target column must be numeric for regression");
                                        }
                                } else {
                                        // Feature columns
                                        if (field.dataType().equals(DataTypes.StringType)) {
                                                categoricalCols.add(fieldName);
                                        } else if (field.dataType().equals(DataTypes.DoubleType) ||
                                                        field.dataType().equals(DataTypes.IntegerType) ||
                                                        field.dataType().equals(DataTypes.FloatType)) {
                                                numericCols.add(fieldName);
                                        }
                                }
                        }

                        System.out.println("Detected label column: " + labelColumn);
                        System.out.println(
                                        "Detected categorical columns: " + Arrays.toString(categoricalCols.toArray()));
                        System.out.println("Detected numerical columns: " + Arrays.toString(numericCols.toArray()));

                        // Data preprocessing pipeline
                        List<PipelineStage> pipelineStages = new ArrayList<>();

                        // Process categorical columns
                        List<String> indexedCols = new ArrayList<>();
                        List<String> encodedCols = new ArrayList<>();

                        for (String categoricalCol : categoricalCols) {
                                // Convert string to index
                                String indexedCol = categoricalCol + "_indexed";
                                StringIndexer indexer = new StringIndexer()
                                                .setInputCol(categoricalCol)
                                                .setOutputCol(indexedCol)
                                                .setHandleInvalid("keep");
                                pipelineStages.add(indexer);
                                indexedCols.add(indexedCol);

                                // Convert indexed values to one-hot encoding
                                String encodedCol = categoricalCol + "_encoded";
                                OneHotEncoder encoder = new OneHotEncoder()
                                                .setInputCol(indexedCol)
                                                .setOutputCol(encodedCol);
                                pipelineStages.add(encoder);
                                encodedCols.add(encodedCol);
                        }

                        // Create feature vector by combining all processed features
                        List<String> assemblerInputs = new ArrayList<>();
                        assemblerInputs.addAll(numericCols);
                        assemblerInputs.addAll(encodedCols);

                        VectorAssembler assembler = new VectorAssembler()
                                        .setInputCols(assemblerInputs.toArray(new String[0]))
                                        .setOutputCol("features");
                        pipelineStages.add(assembler);

                        // Create and add the Linear Regression model to the pipeline
                        LinearRegression lr = new LinearRegression()
                                        .setLabelCol(labelColumn)
                                        .setFeaturesCol("features")
                                        .setMaxIter(100)
                                        .setRegParam(0.1)
                                        .setElasticNetParam(0.0);
                        pipelineStages.add(lr);

                        // Build the pipeline
                        Pipeline pipeline = new Pipeline().setStages(pipelineStages.toArray(new PipelineStage[0]));

                        Dataset<Row> cleanData = data;
                        for (String col : data.columns()) {
                                cleanData = cleanData.na().fill(0.0, new String[] { col }); // Replace nulls with 0.0
                        }
                        data = cleanData;

                        // Split the data into training and test sets
                        Dataset<Row>[] splits = data.randomSplit(new double[] { 0.8, 0.2 }, 42);
                        Dataset<Row> trainingData = splits[0];
                        Dataset<Row> testData = splits[1];

                        System.out.println("Training data count: " + trainingData.count());
                        System.out.println("Test data count: " + testData.count());

                        // Train the model
                        System.out.println("Training the model...");
                        PipelineModel model = pipeline.fit(trainingData);

                        // Make predictions on test data
                        Dataset<Row> predictions = model.transform(testData);

                        // Evaluate the model
                        RegressionEvaluator evaluator = new RegressionEvaluator()
                                        .setLabelCol(labelColumn)
                                        .setPredictionCol("prediction")
                                        .setMetricName("rmse");

                        double rmse = evaluator.evaluate(predictions);

                        // Extract the LinearRegressionModel from the pipeline model
                        LinearRegressionModel lrModel = (LinearRegressionModel) model.stages()[model.stages().length
                                        - 1];

                        // Create a DataFrame to store the model results
                        List<Row> resultRows = new ArrayList<>();
                        resultRows.add(RowFactory.create("RMSE", rmse));
                        resultRows.add(RowFactory.create("R²", lrModel.summary().r2()));
                        resultRows.add(RowFactory.create("Mean Absolute Error", lrModel.summary().meanAbsoluteError()));
                        resultRows.add(RowFactory.create("Mean Squared Error", lrModel.summary().meanSquaredError()));

                        // Create schema for the results DataFrame
                        StructType resultSchema = DataTypes.createStructType(new StructField[] {
                                        DataTypes.createStructField("Metric", DataTypes.StringType, false),
                                        DataTypes.createStructField("Value", DataTypes.DoubleType, false)
                        });

                        Dataset<Row> resultsDf = sparkSession.createDataFrame(resultRows, resultSchema);

                        System.out.println("Model Evaluation Metrics:");
                        resultsDf.show();

                        System.out.println("Saving results to " + outputDir);

                        // Save predictions
                        predictions.select(labelColumn, "prediction").coalesce(1).write()
                                        .option("header", "true")
                                        .mode("overwrite")
                                        .csv(outputDir + "/predictions");

                        // Save metrics
                        resultsDf.coalesce(1).write()
                                        .option("header", "true")
                                        .mode("overwrite")
                                        .csv(outputDir + "/metrics");

                        // Save the feature importance information
                        double[] coefficients = lrModel.coefficients().toArray();

                        List<Row> featureImportanceRows = new ArrayList<>();
                        String[] featureNames = assemblerInputs.toArray(new String[0]);

                        for (int i = 0; i < Math.min(coefficients.length, featureNames.length); i++) {
                                featureImportanceRows.add(RowFactory.create(featureNames[i], coefficients[i]));
                        }

                        StructType featureImportanceSchema = DataTypes.createStructType(new StructField[] {
                                        DataTypes.createStructField("Feature", DataTypes.StringType, false),
                                        DataTypes.createStructField("Coefficient", DataTypes.DoubleType, false)
                        });

                        Dataset<Row> featureImportanceDf = sparkSession.createDataFrame(featureImportanceRows,
                                        featureImportanceSchema);

                        System.out.println("Feature Coefficients:");
                        featureImportanceDf.show();

                        // Save feature importance
                        featureImportanceDf.coalesce(1).write()
                                        .option("header", "true")
                                        .mode("overwrite")
                                        .csv(outputDir + "/feature_importance");

                        System.out.println("Linear regression completed successfully. Results saved to " + outputDir);

                        // Return the results
                        return new LinearRegressionResult(rmse, lrModel.summary().r2(),
                                        lrModel.summary().meanAbsoluteError(), lrModel.summary().meanSquaredError());

                } catch (Exception e) {
                        System.err.println("Error in Spark Linear Regression application: " + e.getMessage());
                        e.printStackTrace();
                        throw e;
                }
        }

        /**
         * Class to hold the results of the linear regression analysis.
         */
        public static class LinearRegressionResult {
                private final double rmse;
                private final double r2;
                private final double mae;
                private final double mse;

                public LinearRegressionResult(double rmse, double r2, double mae, double mse) {
                        this.rmse = rmse;
                        this.r2 = r2;
                        this.mae = mae;
                        this.mse = mse;
                }

                public double getRmse() {
                        return rmse;
                }

                public double getR2() {
                        return r2;
                }

                public double getMae() {
                        return mae;
                }

                public double getMse() {
                        return mse;
                }

                @Override
                public String toString() {
                        return "LinearRegressionResult{" +
                                        "rmse=" + rmse +
                                        ", r2=" + r2 +
                                        ", mae=" + mae +
                                        ", mse=" + mse +
                                        '}';
                }
        }
}
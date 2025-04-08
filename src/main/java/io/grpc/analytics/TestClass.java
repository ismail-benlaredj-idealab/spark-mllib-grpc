package io.grpc.analytics;

import java.io.IOException;

public class TestClass {

 public static void main(String[] args) {

        String dataDirectory = "/home/ismail/grpc-java-examples-master/testFiles"; 
        String filePrefix = "part";
        String outputFilePath = "average_coefficients.csv"; 

        try {
      LinearRegressionAgg.calculateAndSaveAverages(
                                    dataDirectory, 
                                    filePrefix, 
                                    outputFilePath);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}

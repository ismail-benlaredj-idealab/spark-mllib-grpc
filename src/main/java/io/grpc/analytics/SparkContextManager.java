package io.grpc.analytics;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkContextManager {
    private static JavaSparkContext sparkContext = null;
    private static final Object lock = new Object();
    
    public static JavaSparkContext getOrCreateSparkContext() {
        synchronized (lock) {
            // Check if context exists and is not stopped
            if (sparkContext != null && !sparkContext.sc().isStopped()) {
                return sparkContext;
            }
            
            // Create new context if none exists or previous one was stopped
            SparkConf conf = new SparkConf()
                    .setAppName("KMeans Clustering Example")
                    .setMaster("local[*]");
            
            sparkContext = new JavaSparkContext(conf);
            return sparkContext;
        }
    }
    
    public static void stopSparkContext() {
        synchronized (lock) {
            if (sparkContext != null && !sparkContext.sc().isStopped()) {
                sparkContext.stop();
                sparkContext = null;
            }
        }
    }
    
    public static boolean isContextActive() {
        synchronized (lock) {
            return sparkContext != null && !sparkContext.sc().isStopped();
        }
    }
}
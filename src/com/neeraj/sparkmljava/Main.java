/**
 * Main
 */
package com.neeraj.sparkmljava;

import org.apache.spark.api.java.JavaSparkContext;

/**
 * Main Class. To Run Spark Machine Learning Java Examples.
 * 
 * @author neeraj
 *
 */
public class Main {
	/**
	 * Main Class contains the main method to test and run Spark Machine
	 * Learning Java Examples. 
	 * - Spark Linear Regression.
	 * 
	 */

	/**
	 * Runs the Spark Machine Learning Java Examples.
	 * 
	 * @param args
	 *            Takes nothing
	 */
	public static void main(String[] args) {

		/* Creating CreateSpark object to create JavaSparkContext. */
		CreateSpark spark = new CreateSpark();
		JavaSparkContext sparkContext = spark.context("Linear Regression", "local");

		/* Linear Regression */
		SparkLinearRegression linearRegression = new SparkLinearRegression();
		linearRegression.callLR(sparkContext);

		/* Closing sparkContext. */
		sparkContext.close();

	}

}

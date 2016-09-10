/**
 * CreateSpark
 */
package com.neeraj.sparkmljava;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

/**
 * Class CreateSpark. Creates 'JavaSparkContext', 'SparkSession' and 'SQLContext'.
 * 
 * @author neeraj
 *
 */
public class CreateSpark {
	/**
	 * Class CreateSpark implements two functions 'context()', 'session()' and
	 * 'sqlContext()' to create 'JavaSparkContext, SparkSession and SQLContext'.
	 */

	/**
	 * Function 'context()' creates 'JavaSparkContext' using the specified 'Spark
	 * Configuration'.
	 * 
	 * @param appName
	 *            Name of the Spark Application
	 * @param master
	 *            Specifies where the Spark Application runs. Takes values
	 *            'local', local[*], 'master'.
	 * @return JavaSparkContext
	 */
	public JavaSparkContext context(String appName, String master) {

		/* Creating Spark Configuration */
		SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);

		/* Creating Spark Context */
		JavaSparkContext sparkContext = new JavaSparkContext(conf);

		return sparkContext;

	}

	/**
	 * Function 'session()' creates 'SparkSession' using the specified 'Spark
	 * Configuration'.
	 * 
	 * @param appName
	 *            Name of the Spark Application
	 * @param master
	 *            Specifies where the Spark Application runs. Takes values
	 *            'local', local[*], 'master'.
	 * @return SparkSession
	 */
	public SparkSession session(String appName, String master) {

		/* Creating Spark Configuration */
		SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);

		/* Creating Spark Session */
		SparkSession sparkSession = SparkSession.builder().appName(appName).config(conf).getOrCreate();

		return sparkSession;
	}

	/**
	 * Function 'sqlContext()' creates 'SQLContext' using 'JavaSparkContext'.
	 * 
	 * @param sparkContext
	 *            Takes JavaSparkContext from calling method.
	 * @return SQLContext.
	 */
	public SQLContext sqlContext(JavaSparkContext sparkContext) {

		/* Creating SQLContext. */
		@SuppressWarnings("deprecation")
		SQLContext sqlContext = new SQLContext(sparkContext);

		return sqlContext;
	}

}

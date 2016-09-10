/**
 * SparkLinearRegression
 */
package com.neeraj.sparkmljava;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Class SparkLinearRegression. Example on how to do 'Linear Regression' with
 * Apache Spark.
 * 
 * @author neeraj
 *
 */
public class SparkLinearRegression {

	/**
	 * Class SparkLinearRegression contains function 'callLR()' create a model
	 * to predict the mileage of cars. Class SparkLinearRegression contains two
	 * functional classes 'FilteredAttributes' and 'LabeledPoints'.
	 * 
	 */

	/**
	 * Function 'callLR()' uses a data set of car details. Training and Testing
	 * of a model to predict the mileage of cars.
	 * 
	 * @param sparkContext
	 *            Takes JavaSparkContext from calling method.
	 */
	public void callLR(JavaSparkContext sparkContext) {

		/* Creating CreateSpark object to create SQLContext. */
		CreateSpark spark = new CreateSpark();
		SQLContext sqc = spark.sqlContext(sparkContext);
		/* Creating a JavaRDD of String using Car Details. */
		JavaRDD<String> autoData = sparkContext.textFile("Path_to_:cars.csv").cache();

		/* Removing table header. */
		JavaRDD<String> autoData1 = autoData.filter(x -> !x.contains("Model"));

		/* Printing Count of JavaRDD. */
		System.out.println(autoData1.count());

		/* Filtering Attributes and creating a JavaRDD<Vector> */
		JavaRDD<Vector> autoVectorData = autoData1.map(x -> new FilterAttributes().call(x));

		/* Printing JavaRDD<Vector>. */
		autoVectorData.collect().forEach(x -> System.out.println(x));

		/* Creating label points from JavaRDD<Vector>. */
		JavaRDD<Row> autoDataLP = autoVectorData.map(x -> new LabeledPoints().call(x));

		/* Defining the Schema. */
		List<StructField> fields = new ArrayList<>();
		StructField field1 = DataTypes.createStructField("label", DataTypes.DoubleType, true);
		StructField field2 = DataTypes.createStructField("features", new VectorUDT(), true);
		fields.add(field1);
		fields.add(field2);
		StructType schema = DataTypes.createStructType(fields);

		/* Printing schema. */
		schema.printTreeString();

		/* Creating Dataset using labeled points and schema. */
		Dataset<Row> autoDataSet = sqc.createDataFrame(autoDataLP, schema);

		/* Querying the data set to get a table view. */
		autoDataSet.createOrReplaceTempView("auto");
		autoDataSet.sqlContext().sql("SELECT features FROM auto").show(5);
		autoDataSet.select("label").show(5);

		/* Defining data set splitting factors.(Training set, Testing set). */
		double[] weights = { 0.9, 0.1 };

		/* Splitting data set to Training Set and Testing Set. */
		Dataset<Row>[] set = autoDataSet.randomSplit(weights);
		Dataset<Row> train = set[0];
		Dataset<Row> test = set[1];

		/* Creating LinearRegression with maximum iteration 10. */
		LinearRegression lr = new LinearRegression().setMaxIter(10);

		/* Creating model with training set. */
		LinearRegressionModel lrModel = lr.fit(train);

		/* Printing coefficients and intercept of model. */
		System.out.println("Coefficents: " + lrModel.coefficients() + "\nIntercept: " + lrModel.intercept());

		/* Doing prediction using testing set. */
		Dataset<Row> predictions = lrModel.transform(test);

		/* Querying prediction data to get a table view. */
		predictions.select("prediction", "label", "features").show(20);

		/* Creating RegressionEvaluator with metric R2. */
		RegressionEvaluator re = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction")
				.setMetricName("r2");

		/*
		 * Evaluating the prediction. r2>0.7 Good Model. r2>0.5 Ok Model. r2<0.5
		 * Bad Model. Since randomSplit is used on the data set, value of r2
		 * will vary on each execution.
		 */
		double r2 = re.evaluate(predictions);

		/* Printing R2. */
		System.out.println("\n R2 = " + r2 + "\n");

		/*
		 * Training set and Testing set of good models can be saved for future
		 * use.
		 */
		if (r2 > 0.80) {
			train.toJavaRDD().saveAsTextFile("Path_to_Save_/LRDataSet/Train_" + String.valueOf(r2));
			test.toJavaRDD().saveAsTextFile("Path_to_Save_/LRDataSet/Test_" + String.valueOf(r2));
			
			/*Saving to HDFS by using UsingHDFS object. */
			UsingHDFS hdfsData = new UsingHDFS(sparkContext);
			hdfsData.saveToHDFS(train.toJavaRDD(), "Path_to_HDFS");
			hdfsData.saveToHDFS(test.toJavaRDD(), "Path_to_HDFS");
		}
	}

}

/**
 * Class FilterAttributes. To Filter Attributes of car details.
 * 
 * @author neeraj
 *
 */
@SuppressWarnings("serial")
class FilterAttributes implements Function<String, Vector> {
	/**
	 * Class FilterAttributes implements the 'call()' function to filter the
	 * attributes and to return the vector form of it.
	 */

	/**
	 * Function is called from the map transformation to filter the attributes
	 * and to return the vector.
	 * 
	 * @param s
	 *            Takes each record from RDD.
	 */
	public Vector call(String record) {
		/* Splitting string to attributes. */
		String[] attList = record.split(",");
		/* Creating Vector form of required attributes to build model. */
		Vector values = Vectors.dense(Double.valueOf(attList[1]), Double.valueOf(attList[2]),
				Double.valueOf(attList[3]), Double.valueOf(attList[4]), Double.valueOf(attList[5]),
				Double.valueOf(attList[6]), Double.valueOf(attList[7]));
		return values;
	}
}

/**
 * Class LabeledPoints. To create labeled points from vector data of car
 * attributes.
 * 
 * @author neeraj
 *
 */
@SuppressWarnings("serial")
class LabeledPoints implements Function<Vector, Row> {
	/**
	 * Class LabeledPoints implements 'call()' function to convert the attribute
	 * vectors to labeled points.
	 */

	/**
	 * Convert the attribute vectors to labeled points and return them as a row.
	 * 
	 * @param Takes
	 *            each vector from JavaRDD<Vector>
	 */
	public Row call(Vector vector) {
		return RowFactory.create(vector.apply(0), Vectors.dense(vector.apply(1), vector.apply(2), vector.apply(3),
				vector.apply(4), vector.apply(5), vector.apply(6)));
	}
}

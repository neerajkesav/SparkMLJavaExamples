## Spark Machine Learning Java Examples

This project is created to learn Apache Spark Machine Learning using Java. This project consists of the following examples:

  * Linear Regression.

### Data Sets
 * cars.csv - A data set with many attributes of various car models.
 
 ### Getting Started

These instructions will get you a brief idea on setting up the environment and running on your local machine for development and testing purposes. 

**Prerequisities**

- Java
- Apache Spark 
- Hadoop

**Setup and running tests**

1. Run `javac` and `java -version` to check the installation
   
2. Run `spark-shell` and check if Spark is installed properly. 
 
3. Go to Hadoop user (If  installed on different user) and run the following (On Ubuntu Systems): 

      `sudo su hadoopuser`
          
      `start-all.sh`   
           
4. Execute the following commands from terminal to run the tests:

      `javac -classpath "Path to required jar files(spark, hadoop, scala)" Main.java` 
      `java -classpath "Path to required jar files(spark, hadoop, scala)" Main`


###Classes
Please start exploring from Main.java

All classes in this project are listed below:

* **CreateSpark.java** - To create SparkContext, SparkSession and SQLContext. Contains the following methods:
	
      	  `public JavaSparkContext context(String appName, String master)`
      	  `public SparkSession session(String appName, String master)`
		  `public SQLContext sqlContext(JavaSparkContext sparkContext)`
		  
* **SparkLinearRegression.java** - Example on Linear Regression using Spark. Training and testing on a model to predict the mileage of cars. Contains the following method.
	
		  `public void callLR(JavaSparkContext sparkContext)`	

* **UsingHDFS.java** - To save Training and Testing DataSets to HDFS. Contains the following methods:
	
	  	  `public <T> void saveToHDFS(JavaRDD<T> hdfsData, String path)` 
	  	  `public JavaRDD<String> readHDFS(String filePath)`
	
* **Main.java** - Main class to test and run the classes in this project.








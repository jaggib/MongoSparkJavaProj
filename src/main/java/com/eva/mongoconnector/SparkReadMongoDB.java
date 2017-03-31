package com.eva.mongoconnector;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
//import org.apache.spark.sql.DataFrame;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;


public class SparkReadMongoDB {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder()
			      .master("local")
			      .appName("MongoSparkConnectorIntro")
			      .config("spark.mongodb.input.uri", "mongodb://49.19.64.145/EDPP_OBFISCATION_DB.EDPP_OBFISCATION_COLLECTION") // , 27017/test.myCollection
			      .config("spark.mongodb.output.uri", "mongodb://49.19.64.145/EDPP_OBFISCATION_DB.EDPP_OBFISCATION_COLLECTION") // "mongodb://127.0.0.1/test.myCollection
			      .getOrCreate();

			    // Create a JavaSparkContext using the SparkSession's SparkContext object
			    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

			    /*Start Example: Read data from MongoDB************************/
			    JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
			    /*End Example**************************************************/

			    // Analyze data from MongoDB
			    System.out.println(rdd.count());
			    System.out.println(rdd.first().toJson());

			    jsc.close();
			    
	}

}

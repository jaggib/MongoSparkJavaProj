package com.eva.mongoconnector;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import static java.util.Collections.singletonList;


public class SparkMongoAggregation {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder()
			      .master("local")
			      .appName("MongoSparkConnectorIntro")
			      .config("spark.mongodb.input.uri", "mongodb://49.19.64.145/EDPP_OBFISCATION_DB.EDPP_OBFISCATION_COLLECTION") 
			      .config("spark.mongodb.output.uri", "mongodb://49.19.64.145/EDPP_OBFISCATION_DB.EDPP_OBFISCATION_COLLECTION")
			      .getOrCreate();

			    // Create a JavaSparkContext using the SparkSession's SparkContext object
			    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

			    // Load and analyze data from MongoDB
			    JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);

			    /*Start Example: Use aggregation to filter a RDD***************/
			    JavaMongoRDD<Document> aggregatedRdd = rdd.withPipeline(
			      singletonList(
			        Document.parse("{ $match: { customer_id : { $gt : 180 } } }")));
			    /*End Example**************************************************/

			    // Analyze data from MongoDB
			    System.out.println(" total number of entries over 180 : " + aggregatedRdd.count());
			    System.out.println(" all entries over 180 : " + aggregatedRdd.collect());
			    

			    jsc.close();

	}

}

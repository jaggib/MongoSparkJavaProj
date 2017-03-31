package com.eva.mongoconnector;
import com.mongodb.spark.MongoSpark;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;

import org.bson.Document;

import static java.util.Arrays.asList;
public class SparkWriteToMongoDB {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder()
			      .master("local")
			      .appName("MongoSparkConnectorIntro")
			      .config("spark.mongodb.input.uri", "mongodb://49.19.64.145/EDPP_OBFISCATION_DB.EDPP_OBFISCATION_COLLECTION")
			      .config("spark.mongodb.output.uri", "mongodb://49.19.64.145/EDPP_OBFISCATION_DB.EDPP_OBFISCATION_COLLECTION")
			      .getOrCreate();

			    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

			    // Create a RDD of 10 documents
			    JavaRDD<Document> documents = jsc.parallelize(asList(110, 120, 130, 140, 150, 160, 170, 180, 190, 199)).map
			            (new Function<Integer, Document>() {
			      public Document call(final Integer i) throws Exception {
			          return Document.parse("{customer_id: " + i + "}");
			      }
			    });

			    /*Start Example: Save data from RDD to MongoDB*****************/
			    //MongoSpark.save(sparkDocuments, writeConfig);
			    MongoSpark.save(documents);
			    /*End Example**************************************************/

			    jsc.close();

			  }
}

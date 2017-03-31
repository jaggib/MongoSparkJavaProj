package com.eva.mongoconnector;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import com.eva.mapper.JSONPojoMapper;

import scala.Tuple2;

public class MergeAndMaskCustIds {

	public MergeAndMaskCustIds() {
	}

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().master("local").appName("MongoSparkConnectorIntro").
				config("spark.mongodb.input.uri","mongodb://49.19.64.145/EDPP_OBFISCATION_DB.EDPP_OBFISCATION_COLLECTION")
				.config("spark.mongodb.output.uri","mongodb://49.19.64.145/EDPP_OBFISCATION_DB.EDPP_OBFISCATION_COLLECTION")
				.getOrCreate();

		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		// Load and analyze data from MongoDB
		JavaMongoRDD<Document> currentMaskedCustIdsRdd = MongoSpark.load(jsc);

		//JavaRDD allCustIdRdd = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
		// the two sample list are a tuple of cust_id and masked_code, since its a new data set the default masked code is 0.
		 List<Tuple2<String, String>> validAndNewCustIdData = Arrays.asList(new Tuple2("1", "0"),new Tuple2("2", "0") ,new Tuple2("3", "0"),new Tuple2("4", "0"));
		 List<Tuple2<String, String>> invalidAndNewCustIdData = Arrays.asList(new Tuple2("12222", "0"),new Tuple2("222222", "0"),new Tuple2("322222", "0"),new Tuple2("422222", "0"));
		 JavaRDD allCustIdRdd = jsc.parallelize(validAndNewCustIdData);
		 JavaPairRDD<String, String> allCustIdPairRdd = JavaPairRDD.fromJavaRDD(allCustIdRdd) ;

		 JavaRDD rdd2 = jsc.parallelize(invalidAndNewCustIdData);
		 JavaPairRDD<String, String> allJunkCustIdRdd = JavaPairRDD.fromJavaRDD(rdd2) ;

		try {

			// Option to retrieving customer POJO instead of a customer_id as string
			/*currentMaskedCustIdsRdd.map(row -> JSONPojoMapper.getCustomerPojo(row.toJson())).foreach((row) -> {
				System.out.println("Latest : " + row.getCustomerId());
			});*/
			
			// retrieve a list of customer_id/masked_id tuple rdd
			JavaRDD<Tuple2<String, String>> formatedMongoDBCustRDD = currentMaskedCustIdsRdd.map(row -> JSONPojoMapper.getCustomerJasonValue(row.toJson()));
			JavaPairRDD<String, String> formatedMongoDBCustPairedRDD = JavaPairRDD.fromJavaRDD(formatedMongoDBCustRDD);
		
			//List<CustomerPOJO> list = currentMaskedCustIdsRdd.collect();
			
			
			 /*
			  * List<Tuple2<String, String>> list = formatedMongoDBCustRDD.collect();
			  * 
			  * for (Iterator<Tuple2<String, String>> flavoursIter = list.iterator(); flavoursIter.hasNext();){	
				 //System.out.println(currentMaskedCustIdsRdd.collect());
				 //System.out.println( " Latesttttttt====> " + flavoursIter.next() );
			 }*/
			
			 
			 formatedMongoDBCustPairedRDD.join(allCustIdPairRdd).collect().forEach((row) ->
			 {
				 System.out.println(" EQUI JOINED DATA ====>>>   Customer_ID : "+ row._1 + " , Masked_ID :  " + row._2._1);
			 });
			 
			 
			 allJunkCustIdRdd.subtract(formatedMongoDBCustPairedRDD).collect().forEach((row) ->
			 {
				 System.out.println(" NOT-EQUI JOINED DATA ====>>> Customer_ID : "+ row._1 + " , Masked_ID :  " + row._2);
			 });
			 
			
		} catch (Exception e) {
			e.printStackTrace();
		}

		// JavaRDD allinputCustidsRDD = jsc.parallelize(dataStr) ;
		// jsc.parallelize[String](fileLines)

		// Analyze data from MongoDB
		System.out.println(" total number of entries over 180 : " + currentMaskedCustIdsRdd.count());
		System.out.println(" all entries over 180 : " + currentMaskedCustIdsRdd.collect());

		jsc.close();

	}

	public static void printRDD(final JavaRDD<String> s) {
		s.foreach(System.out::println);
	}

}

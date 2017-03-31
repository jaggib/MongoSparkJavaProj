package com.eva.mongoconnector;

import java.util.Map;
import java.util.Set;

import org.bson.BSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class MongoDBJDBC  {

	public static void main(String[] args){
		try{   
			
	         // To connect to mongodb server
	         MongoClient mongoClient = new MongoClient( "49.19.64.145" , 27017 );
				
	         // Now connect to your databases
	         DB db = mongoClient.getDB( "EDPP_OBFISCATION_DB" );
	         System.out.println("Connect to database successfully");
				
	         //boolean auth = db.authenticate(myUserName, myPassword);
	         //System.out.println("Authentication: "+auth);
				
	         DBCollection coll = db.getCollection("EDPP_OBFISCATION_COLLECTION");
	         System.out.println("Collection EDPP_OBFISCATION_COLLECTION	 selected successfully");
				
	         DBCursor cursor = coll.find();
	         int i = 1;
			 int finalCount = 0;	
	         while (cursor.hasNext()) { 
	            System.out.println("Inserted Document: "+i); 
	            System.out.println(cursor.next()); 
	            i++;
	            finalCount = i;
	         }
	         
	        BasicDBObject doc = new BasicDBObject("id", finalCount);
	        coll.insert(doc ) ;
	        System.out.println("Mongo inserted a new object with key : "+ finalCount);

	      }catch(Exception e){
	         System.err.println( e.getClass().getName() + ": " + e.getMessage() );
	      }
	}
}

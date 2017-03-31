package com.eva.mapper;

import java.io.IOException;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonToken;

import scala.Tuple2;
//import org.json4s.jackson.Json;

public class JSONPojoMapper {

	static ObjectMapper mapper = new ObjectMapper();
	//User user = new User();

	//Object to JSON in file
	public static CustomerPOJO getCustomerPojo(final String jsonStr) throws JsonParseException, JsonMappingException, IOException{
		
		 
		
		//CustomerPOJO cust = mapper.readValue(jsonStr, CustomerPOJO.class);
		CustomerPOJO cust = new CustomerPOJO();
		JsonFactory factory = new JsonFactory();

		com.fasterxml.jackson.core.JsonParser jsonParser = factory.createParser(jsonStr);
		
		while(!jsonParser.isClosed()){
		    JsonToken jsonToken = jsonParser.nextToken();

		    //System.out.println("jsonToken = " + jsonToken);
		    if(JsonToken.FIELD_NAME.equals(jsonToken)){
		        String fieldName = jsonParser.getCurrentName();
		        // System.out.println(fieldName);

		        jsonToken = jsonParser.nextToken();

		        if("customer_id".equals(fieldName)){
		        	cust.setCustomerId(jsonParser.getValueAsString());
		        	//System.out.println(" Jackson Mapped Customer Pojo ID is : " + cust.getCustomerId());
		        } 
		        
		        
		    }
		}
		
		return cust;
	}
		
		public static Tuple2<String, String> getCustomerJasonValue(final String jsonStr) throws JsonParseException, JsonMappingException, IOException{
			
			 String customerTagvalue = null;			
			//CustomerPOJO cust = mapper.readValue(jsonStr, CustomerPOJO.class);			
			JsonFactory factory = new JsonFactory();
			com.fasterxml.jackson.core.JsonParser jsonParser = factory.createParser(jsonStr);
			
			while(!jsonParser.isClosed()){
			    JsonToken jsonToken = jsonParser.nextToken();

			    //System.out.println("jsonToken = " + jsonToken);
			    if(JsonToken.FIELD_NAME.equals(jsonToken)){
			        String fieldName = jsonParser.getCurrentName();
			        // System.out.println(fieldName);
			        jsonToken = jsonParser.nextToken();

			        if("customer_id".equals(fieldName)){
			        	//cust.setCustomerId(jsonParser.getValueAsString());
			        	//System.out.println(" Jackson Mapped Customer Pojo ID is : " + cust.getCustomerId());
			        	customerTagvalue = jsonParser.getValueAsString();
			        } 			        			       
			    }
			}
			
			int maskednum = customerTagvalue!=null?customerTagvalue.hashCode():0;
		String maskedVaue = String.valueOf(maskednum);
		return  new Tuple2<String, String>(customerTagvalue,maskedVaue );
		
	//Object to JSON in String
	//String jsonInString = mapper.writeValueAsString(user);
	}
	
}

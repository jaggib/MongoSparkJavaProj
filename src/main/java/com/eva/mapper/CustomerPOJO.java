package com.eva.mapper;

public class CustomerPOJO {

	String customerId;
	String maskedCustomerId;
	
	public CustomerPOJO(String custId , String maskedId ){
		customerId = custId;
		maskedCustomerId = maskedId;
	}
	
	public CustomerPOJO(String custId  ){
		customerId = custId;		
	}
	
	public CustomerPOJO(){
	}
	
	public String getCustomerId() {
		return customerId;
	}
	public void setCustomerId(String customerId) {
		this.customerId = customerId;
	}
	public String getMaskedCustomerId() {
		return maskedCustomerId;
	}
	public void setMaskedCustomerId(String maskedCustomerId) {
		this.maskedCustomerId = maskedCustomerId;
	}
	
	
	
}

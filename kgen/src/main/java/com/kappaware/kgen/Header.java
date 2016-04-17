package com.kappaware.kgen;


public class Header {
	private ExtTs extTs;
	private String partitionKey;
	
	// To be instanciated by json parser
	public Header() {
	}

	public Header(ExtTs extTs, String partitionKey) {
		this.extTs = extTs;
		this.partitionKey = partitionKey;
	}
	
	public ExtTs getExtTs() {
		return extTs;
	}
	public void setExtTs(ExtTs extTs) {
		this.extTs = extTs;
	}

	public String getPartitionKey() {
		return partitionKey;
	}

	public void setPartitionKey(String partitionKey) {
		this.partitionKey = partitionKey;
	}
	
}


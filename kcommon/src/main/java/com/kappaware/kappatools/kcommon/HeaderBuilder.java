package com.kappaware.kappatools.kcommon;

import java.util.HashMap;
import java.util.Map;

public class HeaderBuilder {
	
	private Map<String, Long> counterByKey = new HashMap<String, Long>();
	
	public void build(Header header, ExtTs extTs, String partitionKey) {
		header.setExtTs(extTs);
		header.setPartitionKey(partitionKey);
		Long c = this.counterByKey.get(partitionKey);
		if(c == null) {
			this.counterByKey.put(partitionKey,1L);
			header.setKeyCounter(0L);
		} else {
			header.setKeyCounter(c);
			this.counterByKey.put(partitionKey, c+1);
		}
	}

}

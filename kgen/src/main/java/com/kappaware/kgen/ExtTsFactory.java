package com.kappaware.kgen;

/**
 * Two policy about counter
 * - A simple differentiators for similar timestamp.
 * - A global counter, to allow non-missing messsage check.
 * First option is commented. Second is implemented
 * 
 * @author Serge ALEXANDRE
 *
 */
public class ExtTsFactory {
	private String gateId;
	private long lastCounter = 0;
	
	public ExtTsFactory(String gateId) {
		this.gateId = gateId;
	}

	public ExtTsFactory(String gateId, long initialCounter) {
		this.gateId = gateId;
		this.lastCounter = initialCounter;
	}
	
	public synchronized ExtTs get() {
		long now = System.currentTimeMillis();
		return new ExtTs(now, gateId, lastCounter++); 
	}

}

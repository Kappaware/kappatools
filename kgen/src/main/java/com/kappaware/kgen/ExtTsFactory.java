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
	private long nextCounter = 0;
	
	public ExtTsFactory(String gateId) {
		this.gateId = gateId;
	}

	public ExtTsFactory(String gateId, long initialCounter) {
		this.gateId = gateId;
		this.nextCounter = initialCounter;
	}
	
	public synchronized ExtTs get() {
		long now = System.currentTimeMillis();
		return new ExtTs(now, gateId, nextCounter++); 
	}

	public long getNextCounter() {
		return nextCounter;
	}

}

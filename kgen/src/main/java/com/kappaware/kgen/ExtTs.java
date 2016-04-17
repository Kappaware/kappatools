package com.kappaware.kgen;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class ExtTs implements Comparable<ExtTs> {
	private long timestamp;
	private String gateId;
	private long counter;
	private Integer hashCode;
	
	// Needed for deserializer
	public ExtTs() {
	}
	
	public ExtTs(Long timestamp, String gateId, long counter) {
		super();
		this.timestamp = timestamp;
		this.gateId = gateId;
		this.counter = counter;
	}
	
	public Long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(Long timestamp) {
		this.hashCode = null;
		this.timestamp = timestamp;
	}
	public String getGateId() {
		return gateId;
	}
	public void setGateId(String gateId) {
		this.hashCode = null;
		this.gateId = gateId;
	}
	public long getCounter() {
		return counter;
	}
	public void setCounter(int counter) {
		this.hashCode = null;
		this.counter = counter;
	}

	@Override
	public int compareTo(ExtTs o) {
		if(this.timestamp != o.timestamp) {
			return Long.compare(this.timestamp, o.timestamp);
		} else if(!this.gateId.equals(o.gateId)) {
			return this.gateId.compareTo(o.gateId);
		} else {
			return Long.compare(this.counter, o.counter);
		}
	}
	
	@Override
	public int hashCode() {
		if(this.hashCode == null) {
			this.hashCode = Long.hashCode(this.timestamp) + this.gateId.hashCode() + Long.hashCode(this.counter);
		}
		return this.hashCode;
	}
	
	@Override
	public boolean equals(Object other) {
		if(this.hashCode() != ((ExtTs)other).hashCode()) {
			return false;
		} else {
			return this.timestamp == ((ExtTs)other).timestamp && this.gateId.equals(((ExtTs)other).gateId) && this.counter == ((ExtTs)other).counter;
		}
	}
	
	
	/**
	 * Provide a string representation, for debugging.
	 * Due to this intended usage, we don't care of bottleneck due to synchronization on non-thread safe DataFormatter
	 */
	static DateFormat isoDateFormat;
	static {
		isoDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		isoDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
	}
	@Override
	public String toString() {
		String ts;
		synchronized(isoDateFormat) {
			ts = isoDateFormat.format(new Date(this.timestamp));
		}
		return String.format("%s-%s-%d", ts, this.gateId, this.counter);
	}

}

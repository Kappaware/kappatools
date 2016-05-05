package com.kappaware.kgen.config;


/**
 * as this class is used to biold a json object aims to change a single parameters, we need to use object, not primitive type, to be able to have null values. 
 * @author Serge ALEXANDRE
 *
 */
public class Settings {

	private Long samplingPeriod;
	private Boolean statson;
	private Boolean messon;
	private Integer burstCount;
	private Long period;
	
	// To allow JSON parsing
	Settings() {
	}

	Settings(Parameters params) {
		this.samplingPeriod = params.getSamplingPeriod();
		this.statson = params.isStatson();
		this.messon = params.isMesson();
		this.burstCount = params.getBurstCount();
		this.period = params.getPeriod();
	}
	
	/**
	 * Modify current value with non-null value of another settings
	 * @param s
	 */
	public void apply(Settings s) {
		if(s.samplingPeriod != null) {
			this.samplingPeriod = s.samplingPeriod;
		}
		if(s.statson != null) {
			this.statson = s.statson;
		}
		if(s.messon != null) {
			this.messon = s.messon;
		}
		if(s.burstCount != null) {
			this.burstCount = s.burstCount;
		}
		if(s.period != null) {
			this.period = s.period;
		}
	}
	
	public Long getSamplingPeriod() {
		return samplingPeriod;
	}

	public void setSamplingPeriod(Long samplingPeriod) {
		this.samplingPeriod = samplingPeriod;
	}

	public Boolean getStatson() {
		return statson;
	}

	public void setStatson(Boolean statson) {
		this.statson = statson;
	}

	public Boolean getMesson() {
		return messon;
	}

	public void setMesson(Boolean messon) {
		this.messon = messon;
	}

	public Integer getBurstCount() {
		return burstCount;
	}

	public void setBurstCount(Integer burstCount) {
		this.burstCount = burstCount;
	}

	public Long getPeriod() {
		return period;
	}

	public void setPeriod(Long period) {
		this.period = period;
	}

}

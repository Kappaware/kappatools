/*
 * Copyright (C) 2016 BROADSoftware
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kappaware.kappatools.kcommon.config;


/**
 * as this class is used to build a json object aims to change a single parameters, we need to use object, not primitive type, to be able to have null values. 
 * @author Serge ALEXANDRE
 *
 */
public class Settings {

	private Long samplingPeriod;
	private Boolean statson;
	private Boolean messon;
	
	// To allow JSON parsing
	public Settings() {
	}

	public Settings(Parameters params) {
		this.samplingPeriod = params.getSamplingPeriod();
		this.statson = params.isStatson();
		this.messon = params.isMesson();
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


}

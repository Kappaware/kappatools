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
package com.kappaware.kgen.config;

import com.kappaware.kappatools.kcommon.config.Settings;

/**
 * as this class is used to biold a json object aims to change a single parameters, we need to use object, not primitive type, to be able to have null values. 
 * @author Serge ALEXANDRE
 *
 */
public class SettingsExt extends Settings {

	private Integer burstCount;
	private Long period;
	
	// To allow JSON parsing
	public SettingsExt() {
		super();
	}

	SettingsExt(ParametersImpl params) {
		super(params);
		this.burstCount = params.getBurstCount();
		this.period = params.getPeriod();
	}
	
	/**
	 * Modify current value with non-null value of another settings
	 * @param settings New Settings to apply
	 */
	@Override
	public void apply(Settings settings) {
		super.apply(settings);
		SettingsExt settingsExt = (SettingsExt)settings;
		if(settingsExt.burstCount != null) {
			this.burstCount = settingsExt.burstCount;
		}
		if(settingsExt.period != null) {
			this.period = settingsExt.period;
		}
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

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

package com.kappaware.kappatools.kcommon;

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

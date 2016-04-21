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


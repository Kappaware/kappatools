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
package com.kappaware.k2cassandra.cassandra;

import java.util.Map;

@SuppressWarnings("serial")
public class DbEngineException extends Exception {
	Map<String, Object>  row = null;

	public DbEngineException(String message) {
		super(message);
	}

	public DbEngineException(String message, Throwable t) {
		super(message, t);
	}

	public DbEngineException(String message, Throwable t, Map<String, Object> row) {
		super(message, t);
		this.row = row;
	}

	public Map<String, Object> getRow() {
		return row;
	}

	
}

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

package com.kappaware.king;

import java.io.ByteArrayOutputStream;

import com.kappaware.king.config.Configuration;



/**
 * Note this implementation is for from being optimized, as there is many data copy.
 * 
 * But, this is the safest we can do with this tricky AsyncServlet API. (Need to use the canonical form or entering some issue).
 * 
 * A full re-factoring using Netty (Or akka ?) will be profitable
 * 
 * @author Serge ALEXANDRE
 *
 */
public class ReadBuffer {
	private ByteArrayOutputStream baos;
	private int maxLength;
	private boolean overflow = false;
	
	ReadBuffer(long contentLength, Configuration config) {
		if(contentLength == -1) {
			this.baos = new ByteArrayOutputStream(config.getReadStep());
			this.maxLength = config.getMaxMessageSize();
		} else if (contentLength > config.getMaxMessageSize()) {
			this.baos = new ByteArrayOutputStream(config.getMaxMessageSize());
			this.maxLength = config.getMaxMessageSize();
			this.overflow = true;
		} else {
			this.baos = new ByteArrayOutputStream((int)contentLength);
			this.maxLength = (int)contentLength;
		}
		
	}
	
	void add(byte[] ba, int len) {
		if(baos.size() + len <= maxLength) {
			baos.write(ba, 0, len);
		} else {
			this.overflow = true;
			if(baos.size() < maxLength) {
				baos.write(ba, 0, maxLength - baos.size());
			}
		}
	}

	boolean isOverflow() {
		return overflow;
	}
	
	byte[] getContent() {
		return this.baos.toByteArray();
	}

}

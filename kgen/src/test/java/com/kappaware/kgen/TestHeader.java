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

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.jr.ob.JSON;
import com.fasterxml.jackson.jr.ob.JSONObjectException;
import com.kappaware.kappatools.kcommon.ExtTs;
import com.kappaware.kappatools.kcommon.ExtTsFactory;
import com.kappaware.kappatools.kcommon.Header;
import com.kappaware.kappatools.kcommon.HeaderBuilder;

public class TestHeader {
	private ExtTsFactory extTsFactory;
	private HeaderBuilder headerBuilder;
	private JSON json;
	
	
	@Before
	public void setup() {
		this.extTsFactory = new ExtTsFactory("test2");
		this.headerBuilder = new HeaderBuilder();
		json = JSON.std;
		
	}
	
	static class ExtHeader1 extends Header {
		private String otherStuff;
		private ExtTs otherExtTs;

		public ExtHeader1(ExtTs extTs, HeaderBuilder headerBuilder, String partitionKey) {
			super();
			headerBuilder.build(this, extTs, partitionKey);
		}

		public String getOtherStuff() {
			return otherStuff;
		}

		public void setOtherStuff(String otherStuff) {
			this.otherStuff = otherStuff;
		}

		public ExtTs getOtherExtTs() {
			return otherExtTs;
		}

		public void setOtherExtTs(ExtTs otherExtTs) {
			this.otherExtTs = otherExtTs;
		}
	}

	static class ExtHeader2  {
		private String otherStuff;
		private ExtTs otherExtTs;
		private String partitionKey;
		private ExtTs extTs;

		public ExtHeader2(ExtTs extTs, String partitionKey) {
			this.partitionKey = partitionKey;
			this.extTs = extTs;
		}

		public String getOtherStuff() {
			return otherStuff;
		}

		public void setOtherStuff(String otherStuff) {
			this.otherStuff = otherStuff;
		}

		public ExtTs getOtherExtTs() {
			return otherExtTs;
		}

		public void setOtherExtTs(ExtTs otherExtTs) {
			this.otherExtTs = otherExtTs;
		}

		
		public String getPartitionKey() {
			return partitionKey;
		}
		

		public void setPartitionKey(String partitionKey) {
			this.partitionKey = partitionKey;
		}

		public ExtTs getExtTs() {
			return extTs;
		}

		public void setExtTs(ExtTs extTs) {
			this.extTs = extTs;
		}
		
		
	}
	
	@Test
	public void testSubClassing() throws JSONObjectException, IOException {
		ExtHeader1 extHeader = new ExtHeader1(this.extTsFactory.get(), headerBuilder, "abcd");
		extHeader.setOtherStuff("otherStuff");
		extHeader.setOtherExtTs(this.extTsFactory.get());
		
		String js = this.json.asString(extHeader);
		//System.out.println(js);
		
		Header h = this.json.beanFrom(Header.class, js);
		
		assertEquals("abcd", h.getPartitionKey());
		assertEquals(0, h.getExtTs().getCounter());
		assertEquals("test2", h.getExtTs().getGateId());
	}

	@Test
	public void testAltClassing() throws JSONObjectException, IOException {
		ExtHeader2 extHeader = new ExtHeader2(this.extTsFactory.get(), "abcd");
		extHeader.setOtherStuff("otherStuff");
		extHeader.setOtherExtTs(this.extTsFactory.get());
		
		String js = this.json.asString(extHeader);
		//System.out.println(js);
		
		Header h = this.json.beanFrom(Header.class, js);
		
		assertEquals("abcd", h.getPartitionKey());
		assertEquals(0, h.getExtTs().getCounter());
		assertEquals("test2", h.getExtTs().getGateId());
	}

	
	@Test
	public void testBuilderCount() {
		ExtHeader1 extHeader1 = new ExtHeader1(this.extTsFactory.get(), headerBuilder, "abcd");
		ExtHeader1 extHeader2 = new ExtHeader1(this.extTsFactory.get(), headerBuilder, "1234");
		ExtHeader1 extHeader3 = new ExtHeader1(this.extTsFactory.get(), headerBuilder, "abcd");
		ExtHeader1 extHeader4 = new ExtHeader1(this.extTsFactory.get(), headerBuilder, "abcd");
		ExtHeader1 extHeader5 = new ExtHeader1(this.extTsFactory.get(), headerBuilder, "1234");
		
		assertEquals(0L, extHeader1.getKeyCounter());
		assertEquals(0L, extHeader2.getKeyCounter());
		assertEquals(1L, extHeader3.getKeyCounter());
		assertEquals(2L, extHeader4.getKeyCounter());
		assertEquals(1L, extHeader5.getKeyCounter());
		
		
	}
}

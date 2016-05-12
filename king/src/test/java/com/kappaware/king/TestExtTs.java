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

import java.util.List;
import java.util.Vector;

import com.kappaware.kappatools.kcommon.ExtTs;
import com.kappaware.kappatools.kcommon.ExtTsFactory;

import junit.framework.TestCase;

public class TestExtTs extends TestCase {
	
	@Override
	protected void setUp() {
	}

	@Override
	protected void tearDown() {
	}

	public void test1() {
		ExtTsFactory factory = new ExtTsFactory("g1");
		List<ExtTs> extTss = new Vector<ExtTs>(50);
		int count = 50;
		while(count-- > 0) {
			extTss.add(factory.get());
		}
		// Must ensure generated are unique and incremented
		ExtTs last = null;
		for(ExtTs extTs : extTss) {
			if(last != null) {
				assertFalse(last.equals(extTs));
				assertTrue(last.compareTo(extTs) < 0);
			}
			last = extTs;
		}
	}
}

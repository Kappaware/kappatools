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

package com.kappaware.kappatools.kcommon.jetty;

import java.util.List;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kappaware.kappatools.kcommon.config.ConfigurationException;


/**
 * Spécial range: "*"
 * 
 * The difference with 0.0.0.0/0 is that remote address is not parsed at all. Thus malformed remote address are allowed
 * 
 * @author Serge ALEXANDRE
 *
 */
public class IpMatcherImpl implements IpMatcher {
	static Logger matcherLog = LoggerFactory.getLogger(IpMatcherImpl.class);

	List<IpSegment> segments = new Vector<IpSegment>();
	String range; // Saved only for log message
	private boolean passThrough = false;

	public void setRange(String range) throws ConfigurationException {
		this.range = range;
		if ("*".equals(range.trim())) {
			this.passThrough = true;
		} else {
			segments = new Vector<IpSegment>();
			String[] ranges = range.split("\\s+");
			for (int i = 0; i < ranges.length; i++) {
				segments.add(new IpSegment(ranges[i].trim()));
			}
		}
	}
	
	public void addSegment(String segmentDef) throws ConfigurationException {
		if("*".equals(segmentDef.trim())) {
			this.passThrough = true;
		} else {
			this.segments.add(new IpSegment(segmentDef));
		}
	}

	@Override
	public boolean match(String ipAddress) {
		if (this.passThrough) {
			return true;
		} else {
			long ip;
			try {
				ip = parseIp(ipAddress);
			} catch (Exception e) {
				return false;
			}
			for (IpSegment segment : segments) {
				if (segment.match(ip)) {
					return true;
				}
			}
			return false;
		}
	}

	@Override
	public boolean matchWithLog(String ipAddress, Logger log) {
		if (log == null) {
			log = matcherLog;
		}
		if (this.match(ipAddress)) {
			return true;
		} else {
			log.warn("Remote IP '" + ipAddress + "' does not match any segment in range '" + range + "'");
			return false;
		}
	}

	static private final long B32_MASK = 0xFFFFFFFFL;

	static class IpSegment {
		long maskedAddress;
		long netmask;

		/**
		 * segment is XXX.XXX.XXX.XXX[/YY] (i.e. 192.168.1.0/24)
		 * @param segment
		 * @throws ConfigurationException 
		 */
		IpSegment(String segment) throws ConfigurationException {
			String[] sa = segment.split("/");
			if (sa.length <= 0 || sa.length > 2) {
				throw new ConfigurationException("IP Adress parsing error: '" + segment + "' should be of the form XXX.XXX.XXX.XXX[/NN]");
			}
			if (sa.length == 1) {
				netmask = B32_MASK;
			} else {
				int mask = parseByte(sa[1]);
				if (mask > 32) {
					throw new ConfigurationException("IP Adress parsing error: '" + sa[1] + "' is > 32");
				} else {
					netmask = (B32_MASK << (32 - mask)) & B32_MASK;
				}
			}
			this.maskedAddress = (parseIp(sa[0]) & this.netmask);
		}

		boolean match(long ip) {
			return ((ip & this.netmask) == this.maskedAddress);
		}

	}

	private static long parseIp(String address) throws ConfigurationException {
		String[] sa = address.split("\\.");
		if (sa.length != 4) {
			throw new ConfigurationException("IP Adress parsing error: '" + address + "' does not contains four dot separated blocks");
		}
		return ((parseByte(sa[0]) << 24) | (parseByte(sa[1]) << 16) | (parseByte(sa[2]) << 8) | parseByte(sa[3])) & B32_MASK;
	}

	private static int parseByte(String bs) throws ConfigurationException {
		int x;
		try {
			x = Integer.parseInt(bs);
		} catch (Exception e) {
			throw new ConfigurationException("IP Adress parsing error: '" + bs + "' is not a valid number");
		}
		if (x > 255 || x < 0) {
			throw new ConfigurationException("IP Adress parsing error: '" + bs + "' is > " + 255 + " or negative");
		}
		return x;
	}

}

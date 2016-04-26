package com.kappaware.kcons;

import java.util.Calendar;

import javax.xml.bind.DatatypeConverter;

public class Utils {
	public static String printIsoDateTime(Long ts) {
		if (ts != null) {
			Calendar c = Calendar.getInstance();
			c.setTimeInMillis(ts);
			return DatatypeConverter.printDateTime(c);
		} else {
			return null;
		}
	}
	
}

package it.sweetlab.util;

import java.util.Arrays;
import java.util.Date;

public class Test {

	public static void main(String[] a) {
		logData();
		Object[] o = new Object[15];
		logData();
		System.out.println("riempio l'array");
		for (int i = 0; i<o.length; i++)
			o[i] = "stringa - " + i;
		System.out.println("lo pulisco:");
		logData();
		Arrays.fill(o,null);
		logData();
	}

	private static void logData() {
		System.out.println(DateUtil.toString(new Date(), "HH:mm:ss.SSS"));
	}

}

package it.sweetlab.util;

import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.SQLException;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Locale;

import org.apache.commons.lang.math.NumberUtils;

public class Utils {

	/**
	 * Convert from String to BigDecimal */
	public static BigDecimal toBigDecimal(String value) {
		if(NumberUtils.isNumber(value))
			return new BigDecimal(value);
		else 
			return null;
	}

	/**
	 * Convert from int to BigDecimal */
	public static BigDecimal toBigDecimal(int value) {
		return new BigDecimal(value);
	}

	/**
	 * Convert from Integer to BigDecimal */
	public static BigDecimal toBigDecimal(Integer value) {
		return value==null?null:new BigDecimal(value.intValue());
	}

	/**
	 * Convert from Number to int */
	public static int toInt(Object value) {
		return toInt((Number)value);
	}

	/**
	 * Convert from Number to int */
	public static int toInt(Number value) {
		return value==null?-1:value.intValue();
	}

	/**
	 * Convert from boolean to character (true = 'S' false = 'N') */
	public static String toCharacter(boolean value) {
		return value?"S":"N";
	}

	/**
	 * Convert from character to boolean (true = 'S' false = 'N'). 
	 * Ignore case. */
	public static boolean toBoolean(Object value) {
		return toBoolean((String)value);
	}

	/**
	 * Convert from character to boolean (true = 'S' false = 'N'). 
	 * Ignore case. */
	public static boolean toBoolean(String value) {
		return "S".equalsIgnoreCase(value);
	}
	
	/**
	 * Convert from String to Number, using Locale ITALY */
	public static Float toFloat(String value) {
		try {
			return NumberUtils.createFloat(value);
		} catch(NumberFormatException e) {
			return null;
		}
		
	}
	
	/**
	 * @param n Il numero da formattare.
	 * Utilizzo per default il locale Italy.
	 * @return Il numero sotto forma di stringa formattata.
	 * */
	public static String toString(Number n) {
		return toString(n, Locale.ITALY);
	}

	/**
	 * @param n Il numero da formattare
	 * @param l Il locale con cui formattare il numero.
	 * @return Il numero sotto forma di stringa formattata.
	 */
	public static String toString(Number n, Locale l) {
		NumberFormat numberFormatter = NumberFormat.getNumberInstance(l);
		return numberFormatter.format(n);
	}
	
	/**
	 * @param s La stringa da convertire
	 * @return un nuovo numero (Number) convertito usando il locale Italy.
	 */
	public static Number toNumber(String s) {
		return toNumber(s,Locale.ITALY);
	}

	/**
	 * @param s La stringa da convertire
	 * @param l Il locale con cui effettuare la conversione.
	 * @return un nuovo numero (Number) convertito usando il locale l, 
	 * in argomento. Se vengono sollevate eccezioni, ritorno null.
	 */
	public static Number toNumber(String s, Locale l) {
		NumberFormat numberFormat = NumberFormat.getNumberInstance( l );
		try {
			return numberFormat.parse( s );
		} catch (NumberFormatException e) {
			return null;
		} catch (ParseException e) {
			return null;
		}
	}
	
	/**
	 * @param clob
	 * @return the big string rappresenting CLOB.
	 */
	public static String toString(Clob clob) {
		if (clob==null) return "";
		String result;
		try {
			result = IOUtils.toString(clob.getAsciiStream());
		} catch (SQLException e) {
			result = "";
		}
		return result;
	}
}

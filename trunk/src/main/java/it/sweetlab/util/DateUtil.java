package it.sweetlab.util;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

//import org.apache.log4j.Logger;

public class DateUtil  extends Object {

//  private static Logger sysLog = Logger.getLogger("it.rcs.util");

  public final static String FORMATO_DATA_SHORT      = "yyyyMMdd";
  public final static String FORMATO_TIMESTAMP       = "yyyy-MM-dd HH:mm:ss.S";
  public final static String FORMATO_ORA             = "HH:mm";
  public final static String FORMATO_DATA            = "dd/MM/yyyy";
  public final static String FORMATO_DATA_ORA        =
    FORMATO_DATA+" "+FORMATO_ORA;
  public final static String FORMATO_DATA_INPUT      = "dd/MM/yyyy";
  /** @deprecated */
  public final static String FORMATO_TIMESTAMP_INPUT = "dd/MM/yyyy HH:mm:ss";
  public final static String FORMATO_DATA_ORA_JAVA   = "dd/MM/yyyy HH:mm:ss";
  public final static String FORMATO_ORACLE_DATA     = "dd-mm-yyyy";
  public final static String FORMATO_ORACLE_PRONTA   = "'dd-mm-yyyy'";
  public final static String FORMATO_ORACLE_DATA_ORA_MIN_SEC =
      "dd-mm-yyyy hh24-mi-ss";
  public final static String FORMATO_JAVA_DATA_ORA_MIN_SEC =
      "dd-MM-yyyy HH-mm-ss";

  private final static SimpleDateFormat sdfIn =
    new SimpleDateFormat(FORMATO_DATA_INPUT);
  private final static SimpleDateFormat sdf =
    new SimpleDateFormat(FORMATO_DATA);

  public final static java.sql.Date toSQLDate(Object s, String format){
    return dateToSqlDate(toDate((String)s, format));
  }

  public static final java.sql.Timestamp toSQLTimeStamp(Object s, String format){
    return dateToSqlTimeStamp(toDate((String)s,format));
  }

  public static final java.sql.Timestamp dateToSqlTimeStamp(Date d){
    if (d==null)
      return null;
    else
      return new java.sql.Timestamp(d.getTime());
  }

  public static final java.sql.Date dateToSqlDate(Date d){
    if (d==null)
      return null;
    else
      return new java.sql.Date(d.getTime());
  }

  /**
   * Restituisce una data senza approssimata al giorno.
   * */
  public static final Date getDate(){
    return toDate(fmtString());
  }

  public final static Date toDate(Object s){
    return toDate((String)s, FORMATO_DATA);
  }

  public final static Date toDate(String s){
    return toDate(s, FORMATO_DATA);
  }

  public final static Date toDate(String s, String format) {
    try {
      SimpleDateFormat df = new SimpleDateFormat(format);
      return df.parse(s);
    } catch (Exception e) {
      return null;
    }
  }

  public static final String formattedSqlDate(String format, String sqlFormat){
    return formattedSqlDate(new Date(),format,sqlFormat);
  }

  public static final String formattedSqlDate(Object data, String format,
      String sqlFormat){
    return formattedSqlDate((Date)data,format,sqlFormat);
  }

  public static final String formattedSqlDate(Date d, String format,
      String sqlFormat){
    SimpleDateFormat df = new SimpleDateFormat(format);
    String s;
    if (d != null)
      s="TO_DATE('"+df.format(d)+"','"+sqlFormat+"')";
    else
      s = null;
    return s;
  }

  public final static Timestamp toTimestamp(String strDate){
    Timestamp t = null;
    Date d = sdfIn.parse(strDate, new ParsePosition(0));
    if (d != null){
      t = new Timestamp(d.getTime());
    }
    return t;
  }
  /*
  metodo per aggiungere o sottrarre anni,mesi,giorni,ore,minuti,secondi,millesecondi
  alla data passata in input,in base al valore di field che definisce cosa modificare
  (varia da field=1 modifica dell'anno  a field=14 modifica dei millesecondi)
  amount definisce di quanto modificare la data in input
  */
  public final static Timestamp add(Timestamp dta,int field,int amount){
    Calendar c = Calendar.getInstance();
    c.setTime((Date) dta);
    c.add(field, amount);
    Timestamp t = new Timestamp(c.getTime().getTime());
    return t;
  }

  public final static Timestamp toTimestamp(String strDate, String format){
    SimpleDateFormat sdf = new SimpleDateFormat(format);
    Timestamp t = null;
    Date d = sdf.parse(strDate, new ParsePosition(0));
    if (d != null){
      t = new Timestamp(d.getTime());
    }
    return t;
  }

  public final static String toString(Timestamp t){
    String s = null;
    if (t != null){
      s = sdf.format(t);
    }
    return s;
  }

  public final static String fmtString(){
    return toString(new Date());
  }

  public final static String toString(Date d){
    String s = null;
    if (d != null){
      s = sdf.format(d);
    }
    return s;
  }

  /**
   * Restituisco una data in formato stringa, nella forma dd/MM/yyyy,
   * specificata in @see FORMATO_DATA.
   * */
  public final static String fmtString(Object date){
    if (date instanceof java.lang.String)
      return fmtString((String)date, FORMATO_DATA);
    else if (date instanceof java.util.Date)
      return toString( (Date)date, FORMATO_DATA);
    else
      return toString(new Date(), FORMATO_DATA);
  }

  /**
   * Restituisco una data in formato stringa, nella forma stabilita in fmt
   * */
  public final static String fmtString(String date, String fmt){
    return toString( toDate(date,fmt), fmt );
  }

  public final static String toString(Date d, String format){
    SimpleDateFormat df = new SimpleDateFormat(format);
    String s = null;
    if (d != null){
      s = df.format(d);
    }
    return s;
  }

  public final static String toString(Timestamp t, String format){
    String s = null;
    if (t != null){
      SimpleDateFormat sdf = new SimpleDateFormat(format);
      s = sdf.format(t);
    }
    return s;
  }

	/**
	 * Metodo che effettua il controllo sulla validità di una data.
	 * 
	 * @param date
	 *            La data da controllare
	 * @param format
	 *            Il formato applicato alla data
	 * @return boolean Ritorna la validità della data
	 */
	public static boolean isDate(String date, String format) {
		try {
			SimpleDateFormat dateFormat = new SimpleDateFormat(format);
			dateFormat.parse(date.trim());
			return true;
			// return
			// date.compareTo(dateFormat.format(dateFormat.parse(date.trim())))
			// == 0;
		} catch (ParseException e) {
			return false;
		}
	}

	/**
	 * Metodo che effettua il controllo sulla validità di una data.
	 * 
	 * @param date
	 *            La data da controllare
	 * @param format
	 *            Il formato applicato alla data
	 * @return boolean Ritorna la validità della data
	 */
	public static boolean isNotDate(String date, String format) {
		return !isDate(date, format);
	}

  public static void main(String[] arg){
    Timestamp tmStamp = toTimestamp("12/08/2002");
    System.out.println(tmStamp);//2002-08-12 00:00:00.0
    String str = toString(tmStamp);
    System.out.println(str);//12/08/2002
    String str2 = toString(tmStamp,FORMATO_TIMESTAMP_INPUT);
    System.out.println(str2);// 12/08/02 00:00:00
  }
}
package it.sweetlab.db;

import java.math.BigDecimal;
import java.sql.SQLException;

/**
 * DataBase Utility class. */
public class DBUtils {

	public static int nextVal(String sequenceName, String jndiKey, String jdbcJey) throws SQLException{
		String query = "SELECT "+sequenceName+".NEXTVAL FROM DUAL";
		DataLink dl = new DataLink(jndiKey, jdbcJey);
		int result;
		try {
			result = dl.execCount(query);
		} finally {
			dl.release();
		}
		return result;
	}

	public static int nextVal(String sequenceName) throws SQLException{
		String query = "SELECT "+sequenceName+".NEXTVAL FROM DUAL";
		DataLink dl = new DataLink();
		int result;
		try {
			result = dl.execCount(query);
		} finally {
			dl.release();
		}
		return result;
	}
/*
    public static int nextVal(String sequenceName) throws SQLException{
		return nextVal(sequenceName, DataLink.JNDI_KEY, DataLink.ORACLE_KEY);
	}
*/
	public static BigDecimal bigDecimalNextVal(String sequenceName) throws SQLException{
		return new BigDecimal(nextVal(sequenceName));
	}

	public static BigDecimal bigDecimalNextVal(String sequenceName, String jndiKey, String jdbcJey) throws SQLException{
		return new BigDecimal(nextVal(sequenceName, jndiKey, jdbcJey));
	}
}

package it.sweetlab.db.repository;

import static java.sql.Types.BIGINT;
import static java.sql.Types.BLOB;
import static java.sql.Types.CHAR;
import static java.sql.Types.DATE;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.INTEGER;
import static java.sql.Types.LONGVARCHAR;
import static java.sql.Types.NUMERIC;
import static java.sql.Types.TIME;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.VARCHAR;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;

public abstract class AbstractResolver<T> implements Resolver<T> {

	protected Object getField(int columnType, int columnIndex, ResultSet rSet) throws SQLException{
		ResultSetMetaData meta = rSet.getMetaData();
		switch (columnType) {
			case CHAR: return parseChar(rSet, columnIndex);
			case VARCHAR: return parseVarchar(rSet, columnIndex);
			case LONGVARCHAR: return parseString(rSet, columnIndex);
			case DATE: return parseDate(rSet, columnIndex);
			case TIME: return parseTimes(rSet, columnIndex);
			case TIMESTAMP: return parseDateAndTime(rSet, columnIndex);
			case BIGINT: return parseBigInteger(rSet, columnIndex);
			case DECIMAL: return parseDecimal(rSet, columnIndex, meta.getScale(columnIndex));
			case INTEGER: return parseInteger(rSet, columnIndex, meta.getScale(columnIndex));
			case NUMERIC: return parseNumeric(rSet, columnIndex, meta.getScale(columnIndex));
			case BLOB: return parseBlob(rSet, columnIndex);
			default: return parseUnknown(rSet, columnIndex, columnType, meta);
		}
	}

	protected Object parseChar(ResultSet rSet, int columnIndex) throws SQLException {
		return parseString(rSet, columnIndex);
	}

	protected Object parseVarchar(ResultSet rSet, int columnIndex) throws SQLException {
		return parseString(rSet, columnIndex);
	}
	
	protected Object parseString(ResultSet rSet, int columnIndex) throws SQLException {
		String s = rSet.getString(columnIndex);
		return s == null ? "" : s.trim();
	}
	
	protected Object parseDate(ResultSet rSet, int columnIndex) throws SQLException {
		return parseDateAndTime(rSet, columnIndex);
	}
	
	protected Object parseTimes(ResultSet rSet, int columnIndex) throws SQLException {
		return parseDateAndTime(rSet, columnIndex);
	}
	
	protected Object parseDateAndTime(ResultSet rSet, int columnIndex) throws SQLException {
		Timestamp ts = rSet.getTimestamp(columnIndex);
		if (ts == null) return null;
		return new java.util.Date(ts.getTime());
	}
	
	protected Object parseBigInteger(ResultSet rSet, int columnIndex) throws SQLException {
		return new Long(rSet.getLong(columnIndex));
	}
	
	protected Object parseDecimal(ResultSet rSet, int columnIndex, int scale) throws SQLException {
		return parseNumeric(rSet, columnIndex, scale);
	}

	protected Object parseInteger(ResultSet rSet, int columnIndex, int scale) throws SQLException {
		return parseNumeric(rSet, columnIndex, scale);
	}
	
	protected Object parseNumeric(ResultSet rSet, int columnIndex, int scale) throws SQLException {
		Object o = rSet.getObject(columnIndex);
		if (o == null) return null;
		else if (scale > 0) return new Double(rSet.getDouble(columnIndex));
		else return new Integer(rSet.getInt(columnIndex));
	}
	
	protected Object parseUnknown(ResultSet rSet, int columnIndex, int columnType, ResultSetMetaData meta) throws SQLException {
		return rSet.getObject(columnIndex);
	}
	
	protected Object parseBlob(ResultSet rSet, int columnIndex) throws SQLException {
		throw new UnsupportedOperationException("Blob Parsing implementation is demanded to custom implementation due tu DBMS Implementation License.");
	}
		
}

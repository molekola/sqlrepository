package it.sweetlab.db.repository;

import it.treis.utils.charset.CharactersEncoder;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Date;

public class JSONResolver extends AbstractResolver<String> {

	// new Date(year, month, day, hours, minutes, seconds, milliseconds)
	private static final String DATE_TIME_JSON = "new Date(%1$tY, %1$tm, %1$te, %1$tH, %1$tM, %1$tS, %1$tL)";
	private static final String DATE_TIME_PATTERN = "%1$tY/%1$tm/%1$te %1$tH:%1$tM:%1$tS";

	@Override
	public String resolve(ResultSet resultSet) throws SQLException {
		ResultSetMetaData cols = resultSet.getMetaData();
		StringBuilder sb = new StringBuilder().append("[");
		while (resultSet.next()) {
			sb.append(serializeRow(cols, resultSet)).append(",\n");
		}
		return sb.delete(sb.length()-2, sb.length()).append("]").toString();
	}

	protected StringBuilder serializeRow(ResultSetMetaData cols, ResultSet resultSet) throws SQLException{
		StringBuilder sb = new StringBuilder().append("{");
		for (int i = 1; i <= cols.getColumnCount(); i++){ 
			sb	.append("\"").append(cols.getColumnName(i)).append("\":")
				.append(getField(cols.getColumnType(i), i, resultSet)).append(",");
		}
		return sb.delete(sb.length()-1, sb.length()).append("}");
	}

	@Override
	protected Object parseString(ResultSet rSet, int columnIndex) throws SQLException {
		return "\"" + CharactersEncoder.toHtmlEscapeChar((String)super.parseString(rSet, columnIndex)) + "\"";
	}

	@Override
	protected Object parseDateAndTime(ResultSet rSet, int columnIndex) throws SQLException {
		return String.format(DATE_TIME_JSON, super.parseDateAndTime(rSet, columnIndex));
	}

	public static void main(String[] args) {
		Date d = new Date();
		System.out.println(String.format(DATE_TIME_PATTERN, d));
		System.out.println(String.format(DATE_TIME_JSON, d));
		System.out.println(d);
	}
}

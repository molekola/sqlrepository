package it.sweetlab.db.repository;

import it.sweetlab.data.DataContainer;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BasicResolver extends AbstractResolver<List<Map<String,Object>>> {

	@Override
	public List<Map<String,Object>> resolve(ResultSet resultSet) throws SQLException {
		ResultSetMetaData cols = resultSet.getMetaData();
		DataContainer result = new DataContainer();

		while (resultSet.next()) result.add((Map<String, Object>)getRow(cols, resultSet));

		return result;
	}

	protected Map<String, Object> getRow(ResultSetMetaData cols,ResultSet rSet) throws SQLException{
		HashMap<String, Object> row = new HashMap<String, Object>();
		for (int i = 1; i <= cols.getColumnCount(); i++) row.put(
			cols.getColumnName(i), 
			getField(cols.getColumnType(i), i, rSet)
		);
		return row;
	}

}

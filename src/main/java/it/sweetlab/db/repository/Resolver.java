package it.sweetlab.db.repository;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface Resolver<T> {

	T resolve(ResultSet resultSet) throws SQLException;
	
}

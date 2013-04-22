package service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

public interface JDBCService {

    public String getUpdateStatement(String coreName);

    public Connection getERPDBConnection() throws SQLException;

    public ResultSet getJDBCResultSet(String stmt) throws SQLException;

    public String constructUUIDStringFromRowEntry(List<String> names, HashMap<String, Object> values);
}

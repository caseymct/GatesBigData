package service;

import model.JDBCData;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

public interface JDBCService {

    public Connection getERPDBConnection(JDBCData jdbcData) throws SQLException;

    public ResultSet getJDBCResultSet(JDBCData jdbcData) throws SQLException;

    public String constructUUIDStringFromRowEntry(List<String> values);

    public String constructUUIDStringFromString(String s);

    public String constructUUIDStringFromRowEntry(List<String> names, HashMap<String, Object> values);
}

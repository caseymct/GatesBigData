package service;

import model.JDBCData;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.solr.schema.DateField;

import java.sql.*;
import java.sql.Date;
import java.util.*;

public class JDBCServiceImpl implements JDBCService {

    private static final Logger logger = Logger.getLogger(JDBCServiceImpl.class);

    /* Class.forName ("oracle.jdbc.OracleDriver");
        Connection conn = DriverManager.getConnection("jdbc:oracle:thin:@//erpdbadb.dn.gates.com:1591/ERPDBA", props);
    */
    public Connection getERPDBConnection(JDBCData jdbcData) throws SQLException {
        try {
            Class.forName(jdbcData.getJdbcDriverClass());
            return DriverManager.getConnection(jdbcData.getServerString(), jdbcData.getProperties());
        } catch (ClassNotFoundException e) {
            logger.error(e.getMessage());
        }
        return null;
    }

    public ResultSet getJDBCResultSet(JDBCData jdbcData) throws SQLException {
        Connection conn = getERPDBConnection(jdbcData);
        PreparedStatement pstmt = conn.prepareStatement(jdbcData.getSqlStatement());
        return pstmt.executeQuery();
    }

    private String formatValue(Object val) {
        if (val == null) {
            return null;
        }
        if (val instanceof Date) {
            return DateField.formatExternal((Date) val);
        }
        return val.toString().replaceAll("\\\"", "\\\\\"");
    }

    public String constructUUIDStringFromRowEntry(List<String> values) {
        return UUID.nameUUIDFromBytes(StringUtils.join(values, "").getBytes()).toString();
    }

    public String constructUUIDStringFromString(String s) {
        return UUID.nameUUIDFromBytes(s.getBytes()).toString();
    }

    public String constructUUIDStringFromRowEntry(List<String> names, HashMap<String, Object> values) {
        String uuidString = "";
        for(String name : names) {
            uuidString += name + formatValue(values.get(name));
        }
        return UUID.nameUUIDFromBytes(uuidString.getBytes()).toString();
    }
}

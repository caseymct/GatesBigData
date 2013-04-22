package service;

import GatesBigData.utils.Constants;
import GatesBigData.utils.Utils;
import org.apache.log4j.Logger;
import org.apache.solr.schema.DateField;

import java.sql.*;
import java.sql.Date;
import java.util.*;

public class JDBCServiceImpl implements JDBCService {

    private static final Logger logger = Logger.getLogger(JDBCServiceImpl.class);
    private static HashMap<String, String> updateStatements = new HashMap<String, String>() {{
        put("AR_data",      "select * from apps.xxar_trx_date_1");
        put("AP_data",      "select * from gcca.gcca_ariba_alltables_data");
        put("AP_data_TEST", "select * from gcca.gcca_ariba_alltables_data where INVOICE_ID like '3343408'" +
                            " OR INVOICE_ID like '3343420' OR INVOICE_ID like '3344020' OR INVOICE_ID like '3352014'" +
                            " OR INVOICE_ID like '3391169' OR INVOICE_ID like '3391169' OR INVOICE_ID like '3348012'" +
                            " OR INVOICE_ID like '3348018' OR INVOICE_ID like '3343401'");

    }};

    /*  TNS_ADMIN ERPDBA=(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=erpdbadb.dn.gates.com)(PORT=1591))(CONNECT_DATA=(SID=ERPDBA)))
        Class.forName ("oracle.jdbc.OracleDriver");
        Connection conn = DriverManager.getConnection("jdbc:oracle:thin:@//erpdbadb.dn.gates.com:1591/ERPDBA", props);
    */
    public Connection getERPDBConnection() throws SQLException {
        Properties props = new Properties();
        props.setProperty(Constants.ERP_DB_USERNAME_KEY, Constants.ERP_DB_USERNAME);
        props.setProperty(Constants.ERP_DB_PASSWORD_KEY, Constants.ERP_DB_PASSWORD);

        try {
            Class.forName(Constants.JDBC_DRIVER_CLASS);
            return DriverManager.getConnection(Constants.JDBC_SERVER, props);
        } catch (ClassNotFoundException e) {
            logger.error(e.getMessage());
        }
        return null;
    }

    public String getUpdateStatement(String coreName) {
        return (String) Utils.getObjectIfExists(updateStatements, coreName,  null);
    }

    public ResultSet getJDBCResultSet(String statement) throws SQLException {
        Connection conn = getERPDBConnection();
        PreparedStatement pstmt = conn.prepareStatement(statement);
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

    public String constructUUIDStringFromRowEntry(List<String> names, HashMap<String, Object> values) {
        String uuidString = "";
        for(String name : names) {
            uuidString += name + formatValue(values.get(name));
        }
        return UUID.nameUUIDFromBytes(uuidString.getBytes()).toString();
    }
}

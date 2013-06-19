package model;

import GatesBigData.constants.Constants;
import GatesBigData.constants.JDBC;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/*
TNS_ADMIN ERPDBA=(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=erpdbadb.dn.gates.com)(PORT=1591))(CONNECT_DATA=(SID=ERPDBA)))
ARCHPROD.world  =(DESCRIPTION=(ADDRESS=(COMMUNITY = tcpcom.world)(PROTOCOL = TCP)(Host = dnux062.dn.gates.com)(Port = 1570)))
                 (CONNECT_DATA =(SID = ARCHPROD)(GLOBAL_NAME = archprod.world)(SERVER = DEDICATED)))
*/
public class JDBCData {
    private String collectionName;
    private List<String> tableNames;
    private String userName;
    private String password;
    private String dbSID;
    private String host;
    private int port;
    private String joinByField;
    private String serverString;
    private String sqlStatement;
    private Properties properties;
    
    private String jdbcDBType      = JDBC.DEFAULT_DB_TYPE;
    private String jdbcDriverType  = JDBC.DEFAULT_DRIVER_TYPE;
    private String jdbcDriverClass = JDBC.DEFAULT_DRIVER_CLASS;

    public JDBCData(String collectionName, String tableName, String dbSID, String userName, String password, String host, int port) {
        this(collectionName, Arrays.asList(tableName), dbSID, userName, password, host, port, null);
    }

    public JDBCData(String collectionName, List<String> tableNames, String dbSID, String userName, String password, String host,
                    int port, String joinByField) {
        this.collectionName = collectionName;
        this.tableNames     = tableNames;
        this.dbSID          = dbSID;
        this.userName       = userName;
        this.password       = password;
        this.host           = host;
        this.port           = port;
        this.joinByField    = joinByField;

        initializeProperties();
        initializeSQLStatement();
        constructJDBCServerString();
    }


    public void initializeProperties() {
        this.properties = new Properties();
        this.properties.setProperty(JDBC.DEFAULT_DB_USERNAME_KEY, this.userName);
        this.properties.setProperty(JDBC.DEFAULT_DB_PASSWORD_KEY, this.password);
    }
    
    public void constructJDBCServerString() {
        //= JDBC_PROTOCOL + JDBC_DB_TYPE + ":" + JDBC_DRIVER_TYPE + ":@//" + ERP_DB_SERVER + ":" + ERP_DB_PORT + "/" + ERP_DB_SID;
        this.serverString = Constants.JDBC_PROTOCOL + this.jdbcDBType + ":" + this.jdbcDriverType + ":@//" +
                            this.host + ":" + this.port + "/" + this.dbSID;
    }

    public void setJdbcDBType(String jdbcDBType) {
        this.jdbcDBType = jdbcDBType;
    }

    public void setJdbcDriverType(String jdbcDriverType) {
        this.jdbcDriverType = jdbcDriverType;
    }

    public void setJdbcDriverClass(String jdbcDriverClass) {
        this.jdbcDriverClass = jdbcDriverClass;
    }

    public void initializeSQLStatement() {
        this.sqlStatement = "SELECT * FROM " + StringUtils.join(this.tableNames, ",");

        if (this.tableNames.size() > 1 && this.joinByField != null) {
            List<String> whereClauses = new ArrayList<String>();
            for(int i = 0; i < this.tableNames.size() - 1; i++) {
                whereClauses.add(this.tableNames.get(i) + "." + this.joinByField + "(+) = " +
                                 this.tableNames.get(i+1) + "." + this.joinByField);
            }

            this.sqlStatement += " WHERE " + StringUtils.join(whereClauses, " AND ");
        }
    }

    public String getSqlStatement() {
        return this.sqlStatement;
    }

    public void setSQLStatmentToQueryAllTables() {
        this.sqlStatement = "select table_name from all_tables";
    }


    public void setSQLStatmentToGetCounts() {
        this.sqlStatement = "SELECT COUNT(*) FROM " + StringUtils.join(this.tableNames, ",");
    }

    public void addWhereClauseToSqlStatement(String field, String fieldValue) {
        this.sqlStatement += " WHERE " + field + " like '" + fieldValue + "'";
    }

    public Properties getProperties() {
        return this.properties;
    }
    
    public String getJdbcDriverClass() {
        return this.jdbcDriverClass;
    }
    
    public String getServerString() {
        return this.serverString;
    }
}

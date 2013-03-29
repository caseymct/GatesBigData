package service;

import GatesBigData.utils.Constants;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class JDBCServiceImpl implements JDBCService {

    private static final Logger logger = Logger.getLogger(JDBCServiceImpl.class);
    private CoreService coreService;

    @Autowired
    public void setServices(CoreService coreService) {
        this.coreService = coreService;
    }

    public Connection getConnection() throws SQLException {

        Connection conn = null;
        Properties connectionProps = new Properties();
        connectionProps.put("user", "Look");
        connectionProps.put("password", "look");

        try {
            Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
            DriverManager.getConnection("jdbc:derby://" + Constants.ERP_DB_HOSTNAME + ":" + Constants.ERP_DB_PORT + "/", connectionProps);
        } catch (ClassNotFoundException e) {
            System.out.println(e.getCause());
        }
        conn = DriverManager.getConnection("jdbc:mysql://" + Constants.ERP_DB_HOSTNAME + ":" + Constants.ERP_DB_PORT + "/",
                                            connectionProps);
        return conn;
    }

    public void getData(String coreName) {

        try {
            Connection conn = getConnection();
        } catch (SQLException e) {
            logger.error(e.getMessage());
        }
                /*
                TNS_ADMIN
                ERPDBA=
                (DESCRIPTION=
                        (ADDRESS=(PROTOCOL=tcp)(HOST=erpdbadb.dn.gates.com)(PORT=1591))
        (CONNECT_DATA=
                (SID=ERPDBA)
        )
        )
        */

    }
}

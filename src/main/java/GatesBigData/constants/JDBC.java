package GatesBigData.constants;

import model.JDBCData;

import java.util.Arrays;
import java.util.HashMap;

public class JDBC extends Constants {
    public static final String DEFAULT_DB_TYPE          = "oracle";
    public static final String DEFAULT_DRIVER_TYPE      = "thin";
    public static final String DEFAULT_DRIVER_CLASS     = "oracle.jdbc.OracleDriver";
    public static final String DEFAULT_DB_USERNAME_KEY  = "user";
    public static final String DEFAULT_DB_PASSWORD_KEY  = "password";

    public static final int ERP_DB_PORT                 = 1591;
    public static final String ERP_DB_SERVER            = "erpdbadb.dn.gates.com";
    public static final String ERP_DB_SID               = "ERPDBA";
    public static final String ERP_DB_USERNAME          = "Look";
    public static final String ERP_DB_PASSWORD          = "look";

    public static final int ARCHPROD_DB_PORT            = 1570;
    public static final String ARCHPROD_DB_SERVER       = "dnux062.dn.gates.com";
    public static final String ARCHPROD_DB_SID          = "ARCHPROD.world";
    public static final String ARCHPROD_DB_USERNAME     = "staging";
    public static final String ARCHPROD_DB_PASSWORD     = "bdestaging";
    public static final String ARCHPROD_DB_JOINBY_FIELD = "TXN_ID";



    public static final String JDBC_SERVER              = JDBC_PROTOCOL + DEFAULT_DB_TYPE + ":" + DEFAULT_DRIVER_TYPE + ":@//" + ERP_DB_SERVER + ":" + ERP_DB_PORT + "/" + ERP_DB_SID;

    public static final HashMap<String, JDBCData> JDBC_DATA_HASH_MAP = new HashMap<String, JDBCData>() {{
        put("AR_data_collection",        new JDBCData("AR_data_collection", "apps.xxar_trx_date_1", ERP_DB_SID,
                                                      ERP_DB_USERNAME, ERP_DB_PASSWORD, ERP_DB_SERVER, ERP_DB_PORT));

        put("AP_data_collection",        new JDBCData("AP_data_collection", "gcca.gcca_ariba_alltables_data", ERP_DB_SID,
                                                      ERP_DB_USERNAME, ERP_DB_PASSWORD, ERP_DB_SERVER, ERP_DB_PORT));

        put("Inventory_data_collection", new JDBCData("Inventory_data_collection", "staging.gcca_mtl_txn_dtl",
                                                      ARCHPROD_DB_SID, ARCHPROD_DB_USERNAME, ARCHPROD_DB_PASSWORD, ARCHPROD_DB_SERVER,
                                                      ARCHPROD_DB_PORT));

        put("Inventory_data_collection_joined", new JDBCData("Inventory_data_collection",
                                                    Arrays.asList("staging.gcca_mtl_txn_dtl", "staging.gcca_bde_mtl_txn_acnts"),
                                                    ARCHPROD_DB_SID, ARCHPROD_DB_USERNAME, ARCHPROD_DB_PASSWORD, ARCHPROD_DB_SERVER,
                                                    ARCHPROD_DB_PORT, ARCHPROD_DB_JOINBY_FIELD));
    }};

    public static JDBCData getJDBCData(String collection) {
        return JDBC_DATA_HASH_MAP.containsKey(collection) ? JDBC_DATA_HASH_MAP.get(collection) : null;
    }
}

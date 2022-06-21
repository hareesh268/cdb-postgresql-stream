package com.optum.exts.cdb.stream.transformer;

import com.optum.exts.cdb.stream.config.UtilityConfig;
import com.optum.exts.cdb.stream.utility.ConsumerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.*;

/**
 * Created by rgupta59
 */

@Component
public class CdbSecurityWriteTransformer {

    private static final Logger log = LoggerFactory.getLogger(CdbSecurityWriteTransformer.class);

/*    static {
        try {


            Class.forName("org.postgresql.Driver");


        } catch (ClassNotFoundException e) {
            System.err.println("PostgreSQL DataSource unable to load PostgreSQL JDBC Driver");
        }
    }*/

    @Autowired
    UtilityConfig utilityConfig;
    @Autowired
    ConsumerUtil consumerUtil;
    @Autowired
    JdbcTemplate jdbcTemplate;




    public int insertData(com.optum.exts.cdb.model.key.CDB_SECURITY key, com.optum.exts.cdb.model.CDB_SECURITY cdbSecurity) throws DataAccessException, SQLException {

//log.info("CDB_SECURITY processing key "+key.toString());

        String sql = utilityConfig.getQueryLookup().get("cdbSecurity").replace("<SCHEMA>", utilityConfig.getSchema());


        int[] batch_count= jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {

            @Override
            public int getBatchSize() {
                return 1;
            }

            @Override
            public void setValues(PreparedStatement pstmt, int i) throws SQLException {


                pstmt.setString(1, consumerUtil.removeAsciNull(cdbSecurity.getROLEID()));
                pstmt.setInt(2, cdbSecurity.getPRFLID());
                pstmt.setString(3, consumerUtil.removeAsciNull(cdbSecurity.getAPPLID()));


                pstmt.setString(4, utilityConfig.getSrcSysId());
                pstmt.setTimestamp(5, consumerUtil.getsysTimeStamp());
                pstmt.setTimestamp(6, consumerUtil.getsysTimeStamp());
                pstmt.setString(7, "A");

                pstmt.setString(8, utilityConfig.getSrcSysId());
                pstmt.setTimestamp(9, consumerUtil.getsysTimeStamp());
                pstmt.setString(10, "A");

            }});



        return batch_count[0];


    }

    public int delete(com.optum.exts.cdb.model.key.CDB_SECURITY key )throws DataAccessException, SQLException {
        //String SQL = "UPDATE systest.CNSM_DTL SET row_sts_cd = ?  WHERE partn_nbr=? and cnsm_id=? and src_cd=? and lgcy_src_id = ? ";

        String sql = utilityConfig.getQueryLookup().get("cdbSecurityDel").replace("<SCHEMA>", utilityConfig.getSchema());


        int[] batch_count= jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {

            @Override
            public int getBatchSize() {
                return 1;
            }

            @Override
            public void setValues(PreparedStatement pstmt, int i) throws SQLException {

                pstmt.setString(1, utilityConfig.getPhysicalDelValue());
                pstmt.setTimestamp(2, consumerUtil.getsysTimeStamp());
                pstmt.setString(3, consumerUtil.removeAsciNull(key.getROLEID()));
                pstmt.setString(4, consumerUtil.removeAsciNull(key.getAPPLID()));
                pstmt.setInt(5, key.getPRFLID());

            }});
        return batch_count[0];
    }
}
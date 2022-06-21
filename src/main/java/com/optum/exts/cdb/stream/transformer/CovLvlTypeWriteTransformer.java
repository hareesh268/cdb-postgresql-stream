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
public class CovLvlTypeWriteTransformer {

    private static final Logger log = LoggerFactory.getLogger(CovLvlTypeWriteTransformer.class);


    @Autowired
    UtilityConfig utilityConfig;
    @Autowired
    ConsumerUtil consumerUtil;
    @Autowired
    JdbcTemplate jdbcTemplate;



    public long insertData(com.optum.exts.cdb.model.key.COV_LVL_TYP key, com.optum.exts.cdb.model.COV_LVL_TYP covLvlTyp)throws DataAccessException, SQLException {

//log.info("COV_LVL_TYP processing key "+key.toString());
       /* if((covLvlTyp.getROWUSERID().toString().trim()).equalsIgnoreCase("LINK/UNLINK") && (covLvlTyp.getROWSTSCD().toString().trim()).equalsIgnoreCase("D") ) {

            log.info("Skipping Record due to Link/Unlink Delete  :::" + key);
            return 1;
        }*/
        String sql = utilityConfig.getQueryLookup().get("covLvlTyp").replace("<SCHEMA>", utilityConfig.getSchema());
        ;


        int[] batch_count= jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {

            @Override
            public int getBatchSize() {
                return 1;
            }

            @Override
            public void setValues(PreparedStatement pstmt, int i) throws SQLException {

            pstmt.setString(1, consumerUtil.removeAsciNull(covLvlTyp.getCOVLVLTYPCD()));
            pstmt.setString(2, consumerUtil.removeAsciNull(covLvlTyp.getCOVLVLTYPTXT()));
            pstmt.setString(3, consumerUtil.removeAsciNull(covLvlTyp.getROWUSERID()));
            pstmt.setTimestamp(4, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(covLvlTyp.getROWTMSTMP().toString())));
            pstmt.setString(5, consumerUtil.removeAsciNull(covLvlTyp.getACTVIND()));
            pstmt.setString(6, consumerUtil.removeAsciNull(covLvlTyp.getCOVLVLSHRTTXT()));
            pstmt.setString(7, utilityConfig.getSrcSysId());
            pstmt.setTimestamp(8, consumerUtil.getsysTimeStamp());
            pstmt.setTimestamp(9, consumerUtil.getsysTimeStamp());
            pstmt.setString(10, "A");

            pstmt.setString(11, consumerUtil.removeAsciNull(covLvlTyp.getCOVLVLTYPTXT()));
            pstmt.setTimestamp(12, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(covLvlTyp.getROWTMSTMP().toString())));
            pstmt.setString(13, consumerUtil.removeAsciNull(covLvlTyp.getACTVIND()));
            pstmt.setString(14, consumerUtil.removeAsciNull(covLvlTyp.getCOVLVLSHRTTXT()));
            pstmt.setString(15, utilityConfig.getSrcSysId());
            pstmt.setTimestamp(16, consumerUtil.getsysTimeStamp());
            pstmt.setString(17, "A");
                pstmt.setTimestamp(18,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(covLvlTyp.getROWTMSTMP())));


            }});

        return batch_count[0];
    }


    public int delete(com.optum.exts.cdb.model.key.COV_LVL_TYP  key )throws DataAccessException, SQLException {
        //String SQL = "UPDATE systest.CNSM_DTL SET row_sts_cd = ?  WHERE partn_nbr=? and cnsm_id=? and src_cd=? and lgcy_src_id = ? ";

        String sql = utilityConfig.getQueryLookup().get("covLvlTypDel").replace("<SCHEMA>", utilityConfig.getSchema());

        int affectedrows = 0;

        int[] batch_count = jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {

            @Override
            public int getBatchSize() {
                return 1;
            }

            @Override
            public void setValues(PreparedStatement pstmt, int i) throws SQLException {
                pstmt.setString(1, utilityConfig.getPhysicalDelValue());
                pstmt.setTimestamp(2, consumerUtil.getsysTimeStamp());
                pstmt.setString(3, consumerUtil.removeAsciNull(key.getCOVLVLTYPCD()));


            }
        });

        return batch_count[0];
    }
}
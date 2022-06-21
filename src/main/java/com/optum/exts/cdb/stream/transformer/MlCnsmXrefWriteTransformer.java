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
public class MlCnsmXrefWriteTransformer {

    private static final Logger log = LoggerFactory.getLogger(MlCnsmXrefWriteTransformer.class);


    @Autowired
    UtilityConfig utilityConfig;
    @Autowired
    ConsumerUtil consumerUtil;
    @Autowired
    JdbcTemplate jdbcTemplate;




    public long insertData(com.optum.exts.cdb.model.key.ML_CNSM_XREF key, com.optum.exts.cdb.model.ML_CNSM_XREF mlCnsmXref)throws DataAccessException, SQLException {


        if((mlCnsmXref.getROWUSERID().toString().trim()).equalsIgnoreCase("LINK/UNLINK") && (mlCnsmXref.getROWSTSCD().toString().trim()).equalsIgnoreCase("D") ) {

            log.info("Skipping Record due to Link/Unlink Delete  :::" + key);
            return 1;
        }
        String sql = utilityConfig.getQueryLookup().get("mlCnsmXref").replace("<SCHEMA>", utilityConfig.getSchema());
        ;
        int[] batch_count= jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {

            @Override
            public int getBatchSize() {
                return 1;
            }

            @Override
            public void setValues(PreparedStatement pstmt, int i) throws SQLException {


            pstmt.setInt(1, mlCnsmXref.getPARTNNBR());
            pstmt.setInt(2, mlCnsmXref.getCNSMID());

            pstmt.setString(3, consumerUtil.removeAsciNull(mlCnsmXref.getSRCCD()));
            pstmt.setString(4, consumerUtil.removeAsciNull(mlCnsmXref.getALTCNSMID()));
            pstmt.setString(5, consumerUtil.removeAsciNull(mlCnsmXref.getALTIDTYPCD()));
            pstmt.setString(6, consumerUtil.removeAsciNull(mlCnsmXref.getLGCYSRCID()));
            pstmt.setString(7, consumerUtil.removeAsciNull(mlCnsmXref.getUNFMTALTCNSMID()));

            pstmt.setString(8, consumerUtil.removeAsciNull(mlCnsmXref.getROWSTSCD()));

            pstmt.setString(9, consumerUtil.removeAsciNull(mlCnsmXref.getUPDTTYPCD()));
            pstmt.setString(10, consumerUtil.removeAsciNull(mlCnsmXref.getRACFID()));
            pstmt.setString(11, consumerUtil.removeAsciNull(mlCnsmXref.getROWUSERID()));
            pstmt.setTimestamp(12, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(mlCnsmXref.getROWTMSTMP().toString())));
            pstmt.setString(13, consumerUtil.removeAsciNull(mlCnsmXref.getALPHIDSRCHID()));
            pstmt.setInt(14, mlCnsmXref.getINTIDSRCHID());
            pstmt.setInt(15, mlCnsmXref.getSRCCDBXREFID());
            pstmt.setInt(16, mlCnsmXref.getXREFIDPARTNNBR());
            pstmt.setTimestamp(17, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(mlCnsmXref.getSRCTMSTMP().toString())));
            pstmt.setString(18, utilityConfig.getSrcSysId());
            pstmt.setTimestamp(19, consumerUtil.getsysTimeStamp());
            pstmt.setTimestamp(20, consumerUtil.getsysTimeStamp());

            pstmt.setString(21, consumerUtil.removeAsciNull(mlCnsmXref.getUNFMTALTCNSMID()));

            pstmt.setString(22, consumerUtil.removeAsciNull(mlCnsmXref.getROWSTSCD()));

            pstmt.setString(23, consumerUtil.removeAsciNull(mlCnsmXref.getUPDTTYPCD()));
            pstmt.setString(24, consumerUtil.removeAsciNull(mlCnsmXref.getRACFID()));
            pstmt.setString(25, consumerUtil.removeAsciNull(mlCnsmXref.getROWUSERID()));
            pstmt.setTimestamp(26, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(mlCnsmXref.getROWTMSTMP().toString())));
            pstmt.setString(27, consumerUtil.removeAsciNull(consumerUtil.removeAsciNull(mlCnsmXref.getALPHIDSRCHID())));
            pstmt.setInt(28, mlCnsmXref.getINTIDSRCHID());
            pstmt.setInt(29, mlCnsmXref.getCNSMID());
            pstmt.setInt(30, mlCnsmXref.getPARTNNBR());
            pstmt.setTimestamp(31, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(mlCnsmXref.getSRCTMSTMP().toString())));
            pstmt.setString(32, utilityConfig.getSrcSysId());
            pstmt.setTimestamp(33, consumerUtil.getsysTimeStamp());
                pstmt.setTimestamp(34,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(mlCnsmXref.getROWTMSTMP())));



            }});

        return batch_count[0];
    }


    public int delete(com.optum.exts.cdb.model.key.ML_CNSM_XREF key )throws DataAccessException, SQLException {
        //String SQL = "UPDATE systest.CNSM_DTL SET row_sts_cd = ?  WHERE partn_nbr=? and cnsm_id=? and src_cd=? and lgcy_src_id = ? ";

        //String sql1 = utilityConfig.getQueryLookup().get("lCovPrdtDtDel").replace("<SCHEMA>", utilityConfig.getSchema());

        int affectedrows = 0;
        String sql = utilityConfig.getQueryLookup().get("mlCnsmXrefDel").replace("<SCHEMA>", utilityConfig.getSchema());
        int[] batch_count= jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {

            @Override
            public int getBatchSize() {
                return 1;
            }

            @Override
            public void setValues(PreparedStatement pstmt, int i) throws SQLException {
            pstmt.setString(1, utilityConfig.getPhysicalDelValue());
            pstmt.setTimestamp(2, consumerUtil.getsysTimeStamp());

            pstmt.setInt(3, key.getPARTNNBR());
            pstmt.setInt(4, key.getCNSMID());
            pstmt.setString(5,consumerUtil.removeAsciNull( key.getSRCCD()));
            pstmt.setString(6,consumerUtil.removeAsciNull( key.getLGCYSRCID()));
            pstmt.setString(7,consumerUtil.removeAsciNull( key.getALTCNSMID()));
            pstmt.setString(8, consumerUtil.removeAsciNull(key.getALTIDTYPCD()));

        }});

        return batch_count[0];
    }
}
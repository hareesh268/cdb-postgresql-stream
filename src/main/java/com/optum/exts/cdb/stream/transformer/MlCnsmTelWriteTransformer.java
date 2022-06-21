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
import java.util.zip.DataFormatException;

/**
 * Created by rgupta59
 */

@Component
public class MlCnsmTelWriteTransformer {

    private static final Logger log = LoggerFactory.getLogger(MlCnsmTelWriteTransformer.class);


    @Autowired
    UtilityConfig utilityConfig;
    @Autowired
    ConsumerUtil consumerUtil;
    @Autowired
    JdbcTemplate jdbcTemplate;




    public long insertData(com.optum.exts.cdb.model.key.ML_CNSM_TEL key, com.optum.exts.cdb.model.ML_CNSM_TEL mlCnsmTel)throws DataAccessException, SQLException {


        if((mlCnsmTel.getROWUSERID().toString().trim()).equalsIgnoreCase("LINK/UNLINK") && (mlCnsmTel.getROWSTSCD().toString().trim()).equalsIgnoreCase("D") ) {

            log.info("Skipping Record due to Link/Unlink Delete  :::" + key);
            return 1;
        }
        String sql = utilityConfig.getQueryLookup().get("mlCnsmTel").replace("<SCHEMA>", utilityConfig.getSchema());
        int[] batch_count= jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {

            @Override
            public int getBatchSize() {
                return 1;
            }

            @Override
            public void setValues(PreparedStatement pstmt, int i) throws SQLException {

            pstmt.setInt(1, mlCnsmTel.getPARTNNBR());
            pstmt.setInt(2, mlCnsmTel.getCNSMID());
            pstmt.setString(3, consumerUtil.removeAsciNull(mlCnsmTel.getSRCCD()));
            pstmt.setString(4, consumerUtil.removeAsciNull(mlCnsmTel.getLGCYPOLNBR()));
            pstmt.setString(5, consumerUtil.removeAsciNull(mlCnsmTel.getLGCYSRCID()));
            pstmt.setString(6, consumerUtil.removeAsciNull(mlCnsmTel.getTELTYPCD()));
            pstmt.setString(7, consumerUtil.removeAsciNull(mlCnsmTel.getTELNBR()));
            pstmt.setString(8, consumerUtil.removeAsciNull(mlCnsmTel.getROWSTSCD()));
            pstmt.setString(9, consumerUtil.removeAsciNull(mlCnsmTel.getUPDTTYPCD()));
            pstmt.setString(10, consumerUtil.removeAsciNull(mlCnsmTel.getRACFID()));
            pstmt.setString(11, consumerUtil.removeAsciNull(mlCnsmTel.getROWUSERID()));
            pstmt.setTimestamp(12, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(mlCnsmTel.getROWTMSTMP().toString())));
            pstmt.setTimestamp(13, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(mlCnsmTel.getSRCTMSTMP().toString())));
            pstmt.setInt(14, mlCnsmTel.getSRCCDBXREFID());
            pstmt.setInt(15, mlCnsmTel.getXREFIDPARTNNBR());
            pstmt.setString(16, consumerUtil.removeAsciNull(mlCnsmTel.getTELCLSTYPCD()));
            pstmt.setString(17, utilityConfig.getSrcSysId());
            pstmt.setTimestamp(18, consumerUtil.getsysTimeStamp());
            pstmt.setTimestamp(19, consumerUtil.getsysTimeStamp());


            pstmt.setString(20, consumerUtil.removeAsciNull(mlCnsmTel.getLGCYPOLNBR()));
            pstmt.setString(21, consumerUtil.removeAsciNull(mlCnsmTel.getTELNBR()));
            pstmt.setString(22, consumerUtil.removeAsciNull(mlCnsmTel.getROWSTSCD()));
            pstmt.setString(23, consumerUtil.removeAsciNull(mlCnsmTel.getUPDTTYPCD()));
            pstmt.setString(24, consumerUtil.removeAsciNull(mlCnsmTel.getRACFID()));
            pstmt.setString(25, consumerUtil.removeAsciNull(mlCnsmTel.getROWUSERID()));
            pstmt.setTimestamp(26, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(mlCnsmTel.getROWTMSTMP().toString())));
            pstmt.setTimestamp(27, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(mlCnsmTel.getSRCTMSTMP().toString())));
            pstmt.setInt(28, mlCnsmTel.getCNSMID());
            pstmt.setInt(29, mlCnsmTel.getPARTNNBR());
            pstmt.setString(30, consumerUtil.removeAsciNull(mlCnsmTel.getTELCLSTYPCD()));
            pstmt.setString(31, utilityConfig.getSrcSysId());
            pstmt.setTimestamp(32, consumerUtil.getsysTimeStamp());
                pstmt.setTimestamp(33,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(mlCnsmTel.getROWTMSTMP())));



            }});

        return batch_count[0];
    }


    public int delete(com.optum.exts.cdb.model.key.ML_CNSM_TEL key ) throws DataAccessException, SQLException{
        //String SQL = "UPDATE systest.CNSM_DTL SET row_sts_cd = ?  WHERE partn_nbr=? and cnsm_id=? and src_cd=? and lgcy_src_id = ? ";

        //String sql1 = utilityConfig.getQueryLookup().get("lCovPrdtDtDel").replace("<SCHEMA>", utilityConfig.getSchema());

        int affectedrows = 0;
        String sql = utilityConfig.getQueryLookup().get("mlCnsmTelDel").replace("<SCHEMA>", utilityConfig.getSchema());
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
            pstmt.setString(5, consumerUtil.removeAsciNull(key.getSRCCD()));
            pstmt.setString(6, consumerUtil.removeAsciNull(key.getLGCYSRCID()));
            pstmt.setString(7, consumerUtil.removeAsciNull(key.getTELTYPCD()));



        }});

        return batch_count[0];
    }
}
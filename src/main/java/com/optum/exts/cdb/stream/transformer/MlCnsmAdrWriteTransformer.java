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
public class MlCnsmAdrWriteTransformer {

    private static final Logger log = LoggerFactory.getLogger(MlCnsmAdrWriteTransformer.class);

    /*static {
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




    public long insertData(com.optum.exts.cdb.model.key.ML_CNSM_ADR key, com.optum.exts.cdb.model.ML_CNSM_ADR mlCnsmAdr)throws DataAccessException, SQLException {


        if((mlCnsmAdr.getROWUSERID().toString().trim()).equalsIgnoreCase("LINK/UNLINK") && (mlCnsmAdr.getROWSTSCD().toString().trim()).equalsIgnoreCase("D") ) {

            log.info("Skipping Record due to Link/Unlink Delete  :::" + key);
            return 1;
        }

        String sql = utilityConfig.getQueryLookup().get("mlCnsmAdr").replace("<SCHEMA>", utilityConfig.getSchema());
        ;
        int[] batch_count= jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {

            @Override
            public int getBatchSize() {
                return 1;
            }

            @Override
            public void setValues(PreparedStatement pstmt, int i) throws SQLException {


            pstmt.setInt(1, mlCnsmAdr.getPARTNNBR());
            pstmt.setInt(2, mlCnsmAdr.getCNSMID());
            pstmt.setString(3, consumerUtil.removeAsciNull(mlCnsmAdr.getSRCCD()));
            pstmt.setString(4, consumerUtil.removeAsciNull(mlCnsmAdr.getLGCYPOLNBR()));
            pstmt.setString(5, consumerUtil.removeAsciNull(mlCnsmAdr.getLGCYSRCID()));
            pstmt.setString(6, consumerUtil.removeAsciNull(mlCnsmAdr.getPSTADRTYPCD()));
            pstmt.setString(7, consumerUtil.removeAsciNull(mlCnsmAdr.getSTRADRLN1TXT()));
            pstmt.setString(8, consumerUtil.removeAsciNull(mlCnsmAdr.getSTRADRLN2TXT()));
            pstmt.setString(9, consumerUtil.removeAsciNull(mlCnsmAdr.getCTYNM()));
            pstmt.setString(10, consumerUtil.removeAsciNull(mlCnsmAdr.getPSTCD()));
            pstmt.setString(11, consumerUtil.removeAsciNull(mlCnsmAdr.getPSTEXTCD()));
            pstmt.setString(12, consumerUtil.removeAsciNull(mlCnsmAdr.getSTCD()));
            pstmt.setString(13, consumerUtil.removeAsciNull(mlCnsmAdr.getCNTRYCD()));
            pstmt.setString(14, consumerUtil.removeAsciNull(mlCnsmAdr.getROWSTSCD()));
            pstmt.setString(15, consumerUtil.removeAsciNull(mlCnsmAdr.getUPDTTYPCD()));
            pstmt.setString(16, consumerUtil.removeAsciNull(mlCnsmAdr.getRACFID()));
            pstmt.setString(17, consumerUtil.removeAsciNull(mlCnsmAdr.getROWUSERID()));
            pstmt.setTimestamp(18, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(mlCnsmAdr.getROWTMSTMP().toString())));
            pstmt.setString(19, consumerUtil.removeAsciNull(mlCnsmAdr.getRESMKTSITENBR()));
            pstmt.setDate(20, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(mlCnsmAdr.getPSTADREFFDT().toString())));
            pstmt.setInt(21, mlCnsmAdr.getSRCCDBXREFID());
            pstmt.setInt(22, mlCnsmAdr.getXREFIDPARTNNBR());
            pstmt.setTimestamp(23, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(mlCnsmAdr.getSRCTMSTMP().toString())));
            pstmt.setString(24, consumerUtil.removeAsciNull(mlCnsmAdr.getCNTRYSUBDIVCD()));
            pstmt.setString(25, utilityConfig.getSrcSysId());
            pstmt.setTimestamp(26, consumerUtil.getsysTimeStamp());
            pstmt.setTimestamp(27, consumerUtil.getsysTimeStamp());


            pstmt.setString(28, consumerUtil.removeAsciNull(mlCnsmAdr.getLGCYPOLNBR()));
            pstmt.setString(29, consumerUtil.removeAsciNull(mlCnsmAdr.getSTRADRLN1TXT()));
            pstmt.setString(30, consumerUtil.removeAsciNull(mlCnsmAdr.getSTRADRLN2TXT()));
            pstmt.setString(31, consumerUtil.removeAsciNull(mlCnsmAdr.getCTYNM()));
            pstmt.setString(32, consumerUtil.removeAsciNull(mlCnsmAdr.getPSTCD()));
            pstmt.setString(33, consumerUtil.removeAsciNull(mlCnsmAdr.getPSTEXTCD()));
            pstmt.setString(34, consumerUtil.removeAsciNull(mlCnsmAdr.getSTCD()));
            pstmt.setString(35, consumerUtil.removeAsciNull(mlCnsmAdr.getCNTRYCD()));
            pstmt.setString(36, consumerUtil.removeAsciNull(mlCnsmAdr.getROWSTSCD()));
            pstmt.setString(37, consumerUtil.removeAsciNull(mlCnsmAdr.getUPDTTYPCD()));
            pstmt.setString(38, consumerUtil.removeAsciNull(mlCnsmAdr.getRACFID()));
            pstmt.setString(39, consumerUtil.removeAsciNull(mlCnsmAdr.getROWUSERID()));
            pstmt.setTimestamp(40, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(mlCnsmAdr.getROWTMSTMP().toString())));
            pstmt.setString(41, consumerUtil.removeAsciNull(mlCnsmAdr.getRESMKTSITENBR()));
            pstmt.setDate(42, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(mlCnsmAdr.getPSTADREFFDT().toString())));
            pstmt.setInt(43, mlCnsmAdr.getCNSMID());
            pstmt.setInt(44, mlCnsmAdr.getPARTNNBR());
            pstmt.setTimestamp(45, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(mlCnsmAdr.getSRCTMSTMP().toString())));
            pstmt.setString(46, consumerUtil.removeAsciNull(mlCnsmAdr.getCNTRYSUBDIVCD()));
            pstmt.setString(47, utilityConfig.getSrcSysId());
            pstmt.setTimestamp(48, consumerUtil.getsysTimeStamp());
                pstmt.setTimestamp(49,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(mlCnsmAdr.getROWTMSTMP())));



            }});

        return batch_count[0];
    }


    public int delete(com.optum.exts.cdb.model.key.ML_CNSM_ADR key ) throws DataAccessException, SQLException{
        //String SQL = "UPDATE systest.CNSM_DTL SET row_sts_cd = ?  WHERE partn_nbr=? and cnsm_id=? and src_cd=? and lgcy_src_id = ? ";

        //String sql1 = utilityConfig.getQueryLookup().get("lCovPrdtDtDel").replace("<SCHEMA>", utilityConfig.getSchema());

        int affectedrows = 0;
        String sql = utilityConfig.getQueryLookup().get("mlCnsmAdrDel").replace("<SCHEMA>", utilityConfig.getSchema());

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
            pstmt.setString(7, consumerUtil.removeAsciNull(key.getPSTADRTYPCD()));



        }});

        return batch_count[0];
    }
}
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
public class CnsmMdcrEnrlWriteTransformer {

    private static final Logger log = LoggerFactory.getLogger(CnsmMdcrEnrlWriteTransformer.class);


    @Autowired
    UtilityConfig utilityConfig;
    @Autowired
    ConsumerUtil consumerUtil;

    @Autowired
    JdbcTemplate jdbcTemplate;




    public long insertData(com.optum.exts.cdb.model.key.CNSM_MDCR_ENRL key, com.optum.exts.cdb.model.CNSM_MDCR_ENRL cnsmMdcrEnrl)throws DataAccessException, SQLException {

//log.info("CNSM_MDCR_ENRL processing key "+key.toString());
        if((cnsmMdcrEnrl.getROWUSERID().toString().trim()).equalsIgnoreCase("LINK/UNLINK") && (cnsmMdcrEnrl.getROWSTSCD().toString().trim()).equalsIgnoreCase("D") ) {

            log.info("Skipping Record due to Link/Unlink Delete  :::" + key);
            return 1;
        }

        String sql = utilityConfig.getQueryLookup().get("cnsmMdcrEnrl").replace("<SCHEMA>", utilityConfig.getSchema());
        ;
        int[] batch_count= jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {

            @Override
            public int getBatchSize() {
                return 1;
            }

            @Override
            public void setValues(PreparedStatement pstmt, int i) throws SQLException {


                pstmt.setInt(1, cnsmMdcrEnrl.getPARTNNBR());
                pstmt.setInt(2, cnsmMdcrEnrl.getCNSMID());
                pstmt.setString(3, consumerUtil.removeAsciNull(cnsmMdcrEnrl.getSRCCD()));
                pstmt.setString(4, consumerUtil.removeAsciNull(cnsmMdcrEnrl.getLGCYSRCID()));
                pstmt.setString(5, consumerUtil.removeAsciNull(cnsmMdcrEnrl.getMDCRPARTTYPCD()));
                pstmt.setDate(6, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(cnsmMdcrEnrl.getMDCRENRLPARTEFFDT().toString())));
                pstmt.setDate(7, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(cnsmMdcrEnrl.getMDCRENRLPARTCANCDT().toString())));
                pstmt.setString(8, consumerUtil.removeAsciNull(cnsmMdcrEnrl.getSRCUPDTTYPCD()));
                pstmt.setString(9, consumerUtil.removeAsciNull(cnsmMdcrEnrl.getUPDTRSTRCTYPCD()));
                pstmt.setInt(10, cnsmMdcrEnrl.getSRCCDBXREFID());
                pstmt.setInt(11, cnsmMdcrEnrl.getXREFIDPARTNNBR());
                pstmt.setString(12, consumerUtil.removeAsciNull(cnsmMdcrEnrl.getUPDTTYPCD()));
                pstmt.setString(13, consumerUtil.removeAsciNull(cnsmMdcrEnrl.getRACFID()));
                pstmt.setString(14, consumerUtil.removeAsciNull(cnsmMdcrEnrl.getROWUSERID()));
                pstmt.setString(15, consumerUtil.removeAsciNull(cnsmMdcrEnrl.getROWSTSCD()));
                pstmt.setTimestamp(16, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmMdcrEnrl.getSRCTMSTMP().toString())));
                pstmt.setTimestamp(17, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmMdcrEnrl.getROWTMSTMP().toString())));
                pstmt.setString(18, utilityConfig.getSrcSysId());
                pstmt.setTimestamp(19, consumerUtil.getsysTimeStamp());
                pstmt.setTimestamp(20, consumerUtil.getsysTimeStamp());

                pstmt.setDate(21, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(cnsmMdcrEnrl.getMDCRENRLPARTCANCDT().toString())));
                pstmt.setString(22, consumerUtil.removeAsciNull(cnsmMdcrEnrl.getSRCUPDTTYPCD()));
                pstmt.setString(23, consumerUtil.removeAsciNull(cnsmMdcrEnrl.getUPDTRSTRCTYPCD()));
                pstmt.setInt(24, cnsmMdcrEnrl.getCNSMID());
                pstmt.setInt(25, cnsmMdcrEnrl.getPARTNNBR());
                pstmt.setString(26, consumerUtil.removeAsciNull(cnsmMdcrEnrl.getUPDTTYPCD()));
                pstmt.setString(27, consumerUtil.removeAsciNull(cnsmMdcrEnrl.getRACFID()));
                pstmt.setString(28, consumerUtil.removeAsciNull(cnsmMdcrEnrl.getROWUSERID()));
                pstmt.setString(29, consumerUtil.removeAsciNull(cnsmMdcrEnrl.getROWSTSCD()));
                pstmt.setTimestamp(30, consumerUtil.timeStampParser( consumerUtil.removeAsciNull(cnsmMdcrEnrl.getSRCTMSTMP().toString())));
                pstmt.setTimestamp(31, consumerUtil.timeStampParser( consumerUtil.removeAsciNull(cnsmMdcrEnrl.getROWTMSTMP().toString())));
                pstmt.setString(32, utilityConfig.getSrcSysId());
                pstmt.setTimestamp(33, consumerUtil.getsysTimeStamp());
                pstmt.setTimestamp(34,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmMdcrEnrl.getROWTMSTMP())));



            }});

        return batch_count[0];
    }

    public int delete(com.optum.exts.cdb.model.key.CNSM_MDCR_ENRL key )throws DataAccessException, SQLException {
        //String SQL = "UPDATE systest.CNSM_DTL SET row_sts_cd = ?  WHERE partn_nbr=? and cnsm_id=? and src_cd=? and lgcy_src_id = ? ";

        String sql = utilityConfig.getQueryLookup().get("cnsmMdcrEnrlDel").replace("<SCHEMA>", utilityConfig.getSchema());

        int affectedrows = 0;

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
                pstmt.setString(7, consumerUtil.removeAsciNull(key.getMDCRPARTTYPCD()));
                pstmt.setString(8, consumerUtil.removeAsciNull(key.getMDCRENRLPARTEFFDT().toString()));



            }});
        return batch_count[0];
    }
}
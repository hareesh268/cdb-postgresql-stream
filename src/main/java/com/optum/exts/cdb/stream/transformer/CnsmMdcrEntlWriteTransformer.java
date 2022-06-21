package com.optum.exts.cdb.stream.transformer;

import com.optum.exts.cdb.model.key.CNSM_MDCR_ENTL;
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
public class CnsmMdcrEntlWriteTransformer {

    private static final Logger log = LoggerFactory.getLogger(CnsmMdcrEntlWriteTransformer.class);



    @Autowired
    UtilityConfig utilityConfig;
    @Autowired
    ConsumerUtil consumerUtil;
    @Autowired
    JdbcTemplate jdbcTemplate;




    public long insertData(CNSM_MDCR_ENTL key, com.optum.exts.cdb.model.CNSM_MDCR_ENTL cnsmMdcrEntl)throws DataAccessException, SQLException {

//log.info("CNSM_MDCR_ENTL processing key "+key.toString());
       /* String SQL = "INSERT INTO csptest.subscriberAddres () "
                + "VALUES(?,?,?,?,?,?,?,?,?,?)";*/
        if((cnsmMdcrEntl.getROWUSERID().toString().trim()).equalsIgnoreCase("LINK/UNLINK") && (cnsmMdcrEntl.getROWSTSCD().toString().trim()).equalsIgnoreCase("D") ) {

            log.info("Skipping Record due to Link/Unlink Delete  :::" + key);
            return 1;
        }

        String sql = utilityConfig.getQueryLookup().get("cnsmMdcrEntl").replace("<SCHEMA>", utilityConfig.getSchema());
        ;
        int[] batch_count= jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {

            @Override
            public int getBatchSize() {
                return 1;
            }

            @Override
            public void setValues(PreparedStatement pstmt, int i) throws SQLException {


                pstmt.setInt(1, cnsmMdcrEntl.getPARTNNBR());
                pstmt.setInt(2, cnsmMdcrEntl.getCNSMID());
                pstmt.setString(3, consumerUtil.removeAsciNull(cnsmMdcrEntl.getSRCCD()));
                pstmt.setString(4, consumerUtil.removeAsciNull(cnsmMdcrEntl.getLGCYSRCID()));
                pstmt.setString(5, consumerUtil.removeAsciNull(cnsmMdcrEntl.getENTLTYPCD()));
                pstmt.setDate(6, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(cnsmMdcrEntl.getENTLEFFDT().toString())));
                pstmt.setDate(7, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(cnsmMdcrEntl.getENTLCANCDT().toString())));
                pstmt.setDate(8, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(cnsmMdcrEntl.getFSTDLSDT().toString())));
                pstmt.setDate(9, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(cnsmMdcrEntl.getTPLNTDT().toString())));
                pstmt.setString(10, consumerUtil.removeAsciNull(cnsmMdcrEntl.getSRCUPDTTYPCD()));
                pstmt.setString(11, consumerUtil.removeAsciNull(cnsmMdcrEntl.getUPDTRSTRCTYPCD()));
                pstmt.setInt(12, cnsmMdcrEntl.getSRCCDBXREFID());
                pstmt.setInt(13, cnsmMdcrEntl.getXREFIDPARTNNBR());
                pstmt.setString(14, consumerUtil.removeAsciNull(cnsmMdcrEntl.getUPDTTYPCD()));
                pstmt.setString(15, consumerUtil.removeAsciNull(cnsmMdcrEntl.getRACFID()));
                pstmt.setString(16, consumerUtil.removeAsciNull(cnsmMdcrEntl.getROWUSERID()));
                pstmt.setString(17, consumerUtil.removeAsciNull(cnsmMdcrEntl.getROWSTSCD()));
                pstmt.setTimestamp(18, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmMdcrEntl.getSRCTMSTMP().toString())));
                pstmt.setTimestamp(19, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmMdcrEntl.getROWTMSTMP().toString())));
                pstmt.setString(20, utilityConfig.getSrcSysId());
                pstmt.setTimestamp(21, consumerUtil.getsysTimeStamp());
                pstmt.setTimestamp(22, consumerUtil.getsysTimeStamp());
                //update statements
                pstmt.setDate(23, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(cnsmMdcrEntl.getENTLCANCDT().toString())));
                pstmt.setDate(24, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(cnsmMdcrEntl.getFSTDLSDT().toString())));
                pstmt.setDate(25, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(cnsmMdcrEntl.getTPLNTDT().toString())));
                pstmt.setString(26, consumerUtil.removeAsciNull(cnsmMdcrEntl.getSRCUPDTTYPCD()));
                pstmt.setString(27, consumerUtil.removeAsciNull(cnsmMdcrEntl.getUPDTRSTRCTYPCD()));
                pstmt.setInt(28, cnsmMdcrEntl.getCNSMID());
                pstmt.setInt(29, cnsmMdcrEntl.getPARTNNBR());
                pstmt.setString(30, consumerUtil.removeAsciNull(cnsmMdcrEntl.getUPDTTYPCD()));
                pstmt.setString(31, consumerUtil.removeAsciNull(cnsmMdcrEntl.getRACFID()));
                pstmt.setString(32, consumerUtil.removeAsciNull(cnsmMdcrEntl.getROWUSERID()));
                pstmt.setString(33, consumerUtil.removeAsciNull(cnsmMdcrEntl.getROWSTSCD()));
                pstmt.setTimestamp(34, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmMdcrEntl.getSRCTMSTMP().toString())));
                pstmt.setTimestamp(35, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmMdcrEntl.getROWTMSTMP().toString())));
                pstmt.setString(36, utilityConfig.getSrcSysId());
                pstmt.setTimestamp(37, consumerUtil.getsysTimeStamp());
                pstmt.setTimestamp(38,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmMdcrEntl.getROWTMSTMP())));


            }});

        return batch_count[0];
        }

    public int delete(CNSM_MDCR_ENTL key ) throws DataAccessException, SQLException{
        //String SQL = "UPDATE systest.CNSM_DTL SET row_sts_cd = ?  WHERE partn_nbr=? and cnsm_id=? and src_cd=? and lgcy_src_id = ? ";

        String sql = utilityConfig.getQueryLookup().get("cnsmMdcrEntlDel").replace("<SCHEMA>", utilityConfig.getSchema());

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
                pstmt.setInt(3, key.getPARTNNBR());
                pstmt.setInt(4, key.getCNSMID());
                pstmt.setString(5, consumerUtil.removeAsciNull(key.getSRCCD()));
                pstmt.setString(6, consumerUtil.removeAsciNull(key.getLGCYSRCID()));
                pstmt.setString(7, consumerUtil.removeAsciNull(key.getENTLTYPCD()));
                pstmt.setString(8,consumerUtil.removeAsciNull( key.getENTLEFFDT().toString()));

            }
        });

        return batch_count[0];
    }
}
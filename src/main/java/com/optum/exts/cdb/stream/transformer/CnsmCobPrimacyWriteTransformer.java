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

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by rgupta59
 */

@Component
public class CnsmCobPrimacyWriteTransformer {

    private static final Logger log = LoggerFactory.getLogger(CnsmCobPrimacyWriteTransformer.class);



    @Autowired
    UtilityConfig utilityConfig;
    @Autowired
    ConsumerUtil consumerUtil;
    @Autowired
    JdbcTemplate jdbcTemplate;




    public long insertData(com.optum.exts.cdb.model.key.CNSM_COB_PRIMACY key, com.optum.exts.cdb.model.CNSM_COB_PRIMACY cnsmCobPrimacy)throws DataAccessException, SQLException {
        if((cnsmCobPrimacy.getROWUSERID().toString().trim()).equalsIgnoreCase("LINK/UNLINK") && (cnsmCobPrimacy.getROWSTSCD().toString().trim()).equalsIgnoreCase("D") ) {

            log.info("Skipping Record due to Link/Unlink Delete  :::" + key);
            return 1;
        }

//log.info("CNSM_COB_PRIMACY processing key "+key.toString());

        String sql = utilityConfig.getQueryLookup().get("cnsmCobPrimacy").replace("<SCHEMA>", utilityConfig.getSchema());
        ;
        long id = 0;

        int[] batch_count= jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {

            @Override
            public int getBatchSize() {
                return 1;
            }

            @Override
            public void setValues(PreparedStatement pstmt, int i) throws SQLException {

                pstmt.setInt(1,cnsmCobPrimacy.getXREFIDPARTNNBR());
                pstmt.setInt(2,cnsmCobPrimacy.getSRCCDBXREFID());
                pstmt.setString(3,consumerUtil.removeAsciNull(cnsmCobPrimacy.getSRCCD()));
                pstmt.setString(4,consumerUtil.removeAsciNull(cnsmCobPrimacy.getLGCYSRCID()));
                pstmt.setString(5,consumerUtil.removeAsciNull(cnsmCobPrimacy.getCOBCOVTYPCD()));
                pstmt.setString(6,consumerUtil.removeAsciNull(cnsmCobPrimacy.getOITYPCD()));
                pstmt.setDate(7,consumerUtil.dateParser(consumerUtil.removeAsciNull(cnsmCobPrimacy.getCOBPRIMACYEFFDT())));
                pstmt.setDate(8,consumerUtil.dateParser(consumerUtil.removeAsciNull(cnsmCobPrimacy.getCOBPRIMACYCANCDT())));
                pstmt.setString(9,consumerUtil.removeAsciNull(cnsmCobPrimacy.getOIPRIMACYTYPCD()));
                pstmt.setString(10,consumerUtil.removeAsciNull(cnsmCobPrimacy.getOIPRIMACYRSNCD()));
                pstmt.setString(11,consumerUtil.removeAsciNull(cnsmCobPrimacy.getUHGORDRBENTYPCD()));
                pstmt.setDate(12,consumerUtil.dateParser(cnsmCobPrimacy.getACCDT()));
                pstmt.setString(13,consumerUtil.removeAsciNull(cnsmCobPrimacy.getOIPOLNBR()));
                pstmt.setString(14,consumerUtil.removeAsciNull(cnsmCobPrimacy.getOIPOLTYPCD()));
                pstmt.setString(15,consumerUtil.removeAsciNull(cnsmCobPrimacy.getOICRDHLDRID()));
                pstmt.setString(16,consumerUtil.removeAsciNull(cnsmCobPrimacy.getOIGRPNM()));
                pstmt.setString(17,consumerUtil.removeAsciNull(cnsmCobPrimacy.getRXPCNNBR()));
                pstmt.setString(18,consumerUtil.removeAsciNull(cnsmCobPrimacy.getRXBINNBR()));
                pstmt.setString(19,consumerUtil.removeAsciNull(cnsmCobPrimacy.getOINBR()));
                pstmt.setString(20,consumerUtil.removeAsciNull(cnsmCobPrimacy.getSRCUPDTTYPCD()));
                pstmt.setString(21,consumerUtil.removeAsciNull(cnsmCobPrimacy.getUPDTRSTRCTYPCD()));
                pstmt.setString(22,consumerUtil.removeAsciNull(cnsmCobPrimacy.getUPDTTYPCD()));
                pstmt.setString(23,consumerUtil.removeAsciNull(cnsmCobPrimacy.getRACFID()));
                pstmt.setString(24,consumerUtil.removeAsciNull(cnsmCobPrimacy.getROWUSERID()));
                pstmt.setString(25,consumerUtil.removeAsciNull(cnsmCobPrimacy.getROWSTSCD()));
                pstmt.setTimestamp(26,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmCobPrimacy.getSRCTMSTMP())));
                pstmt.setTimestamp(27,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmCobPrimacy.getROWTMSTMP())));
                pstmt.setString(28,consumerUtil.removeAsciNull(cnsmCobPrimacy.getRXCARRTELNBR()));
                pstmt.setString(29,consumerUtil.removeAsciNull(cnsmCobPrimacy.getRXGRPNBR()));
                pstmt.setString(30,consumerUtil.removeAsciNull(cnsmCobPrimacy.getOICRDHLDRFSTNM()));
                pstmt.setString(31,consumerUtil.removeAsciNull(cnsmCobPrimacy.getOICRDHLDRLSTNM()));
                pstmt.setString(32,consumerUtil.removeAsciNull(cnsmCobPrimacy.getOICRDHLDRMIDLNM()));
                pstmt.setString(33,consumerUtil.removeAsciNull(cnsmCobPrimacy.getOICRDHLDRNMSUFXCD()));
                pstmt.setString(34,consumerUtil.removeAsciNull(cnsmCobPrimacy.getOICRDHLDRTELNBR()));
                pstmt.setInt(35,cnsmCobPrimacy.getPARTNNBR());
                pstmt.setInt(36,cnsmCobPrimacy.getCNSMID());
                pstmt.setString(37,consumerUtil.removeAsciNull(utilityConfig.getSrcSysId()));
                pstmt.setTimestamp(38,consumerUtil.getsysTimeStamp());
                pstmt.setTimestamp(39,consumerUtil.getsysTimeStamp());


                pstmt.setDate(40,consumerUtil.dateParser(consumerUtil.removeAsciNull(cnsmCobPrimacy.getCOBPRIMACYCANCDT())));
                pstmt.setString(41,consumerUtil.removeAsciNull(cnsmCobPrimacy.getOIPRIMACYTYPCD()));
                pstmt.setString(42,consumerUtil.removeAsciNull(cnsmCobPrimacy.getOIPRIMACYRSNCD()));
                pstmt.setString(43,consumerUtil.removeAsciNull(cnsmCobPrimacy.getUHGORDRBENTYPCD()));
                pstmt.setDate(44,consumerUtil.dateParser(consumerUtil.removeAsciNull(cnsmCobPrimacy.getACCDT())));
                pstmt.setString(45,consumerUtil.removeAsciNull(cnsmCobPrimacy.getOIPOLNBR()));
                pstmt.setString(46,consumerUtil.removeAsciNull(cnsmCobPrimacy.getOIPOLTYPCD()));
                pstmt.setString(47,consumerUtil.removeAsciNull(cnsmCobPrimacy.getOICRDHLDRID()));
                pstmt.setString(48,consumerUtil.removeAsciNull(cnsmCobPrimacy.getOIGRPNM()));
                pstmt.setString(49,consumerUtil.removeAsciNull(cnsmCobPrimacy.getRXPCNNBR()));
                pstmt.setString(50,consumerUtil.removeAsciNull(cnsmCobPrimacy.getRXBINNBR()));
                pstmt.setString(51,consumerUtil.removeAsciNull(cnsmCobPrimacy.getOINBR()));
                pstmt.setString(52,consumerUtil.removeAsciNull(cnsmCobPrimacy.getSRCUPDTTYPCD()));
                pstmt.setString(53,consumerUtil.removeAsciNull(cnsmCobPrimacy.getUPDTRSTRCTYPCD()));
                pstmt.setString(54,consumerUtil.removeAsciNull(cnsmCobPrimacy.getUPDTTYPCD()));
                pstmt.setString(55,consumerUtil.removeAsciNull(cnsmCobPrimacy.getRACFID()));
                pstmt.setString(56,consumerUtil.removeAsciNull(cnsmCobPrimacy.getROWUSERID()));
                pstmt.setString(57,consumerUtil.removeAsciNull(cnsmCobPrimacy.getROWSTSCD()));
                pstmt.setTimestamp(58,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmCobPrimacy.getSRCTMSTMP())));
                pstmt.setTimestamp(59,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmCobPrimacy.getROWTMSTMP())));
                pstmt.setString(60,consumerUtil.removeAsciNull(cnsmCobPrimacy.getRXCARRTELNBR()));
                pstmt.setString(61,consumerUtil.removeAsciNull(cnsmCobPrimacy.getRXGRPNBR()));
                pstmt.setString(62,consumerUtil.removeAsciNull(cnsmCobPrimacy.getOICRDHLDRFSTNM()));
                pstmt.setString(63,consumerUtil.removeAsciNull(cnsmCobPrimacy.getOICRDHLDRLSTNM()));
                pstmt.setString(64,consumerUtil.removeAsciNull(cnsmCobPrimacy.getOICRDHLDRMIDLNM()));
                pstmt.setString(65,consumerUtil.removeAsciNull(cnsmCobPrimacy.getOICRDHLDRNMSUFXCD()));
                pstmt.setString(66,consumerUtil.removeAsciNull(cnsmCobPrimacy.getOICRDHLDRTELNBR()));
                pstmt.setInt(67,cnsmCobPrimacy.getPARTNNBR());
                pstmt.setInt(68,cnsmCobPrimacy.getCNSMID());
                pstmt.setString(69,consumerUtil.removeAsciNull(utilityConfig.getSrcSysId()));
                pstmt.setTimestamp(70,consumerUtil.getsysTimeStamp());
                pstmt.setTimestamp(71,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmCobPrimacy.getROWTMSTMP())));



            }});

        return batch_count[0];
    }

    public int delete(com.optum.exts.cdb.model.key.CNSM_COB_PRIMACY key ) throws DataAccessException, SQLException{
        //String SQL = "UPDATE systest.CNSM_DTL SET row_sts_cd = ?  WHERE partn_nbr=? and cnsm_id=? and src_cd=? and lgcy_src_id = ? ";

        String sql = utilityConfig.getQueryLookup().get("cnsmCobPrimacyDel").replace("<SCHEMA>", utilityConfig.getSchema());

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
                pstmt.setInt(3,key.getPARTNNBR());
                pstmt.setInt(4,key.getCNSMID());
                pstmt.setString(5,consumerUtil.removeAsciNull(key.getSRCCD()));
                pstmt.setString(6,consumerUtil.removeAsciNull(key.getLGCYSRCID()));
                pstmt.setString(7,consumerUtil.removeAsciNull(key.getCOBCOVTYPCD()));
                pstmt.setString(8,consumerUtil.removeAsciNull(key.getOITYPCD()));
                pstmt.setDate(9,consumerUtil.dateParser(consumerUtil.removeAsciNull(key.getCOBPRIMACYEFFDT())));

        }});

        return batch_count[0];
    }
}
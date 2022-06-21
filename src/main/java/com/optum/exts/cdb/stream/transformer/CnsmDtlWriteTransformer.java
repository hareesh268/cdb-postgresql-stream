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
public class CnsmDtlWriteTransformer {

    private static final Logger log = LoggerFactory.getLogger(CnsmDtlWriteTransformer.class);


    @Autowired
    UtilityConfig utilityConfig;
    @Autowired
    ConsumerUtil consumerUtil;
    @Autowired
    JdbcTemplate jdbcTemplate;




    public long insertData(com.optum.exts.cdb.model.key.CNSM_DTL key, com.optum.exts.cdb.model.CNSM_DTL cnsmDtl)throws DataAccessException, SQLException {

//log.info("CNSM_DTL processing key "+key.toString());

        if((cnsmDtl.getROWUSERID().toString().trim()).equalsIgnoreCase("LINK/UNLINK") && (cnsmDtl.getROWSTSCD().toString().trim()).equalsIgnoreCase("D") ) {

            log.info("Skipping Record due to Link/Unlink Delete  :::" + key);
            return 1;
        }


        String sql = utilityConfig.getQueryLookup().get("cnsmDtl").replace("<SCHEMA>", utilityConfig.getSchema());
        ;
        int[] batch_count= jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {

                    @Override
                    public int getBatchSize() {
                        return 1;
                    }

                    @Override
                    public void setValues(PreparedStatement pstmt, int i) throws SQLException {

            pstmt.setInt(1, cnsmDtl.getPARTNNBR());
            pstmt.setInt(2, cnsmDtl.getCNSMID());
            pstmt.setString(3,  consumerUtil.removeAsciNull(cnsmDtl.getSRCCD()));
            pstmt.setString(4,  consumerUtil.removeAsciNull(cnsmDtl.getLGCYSRCID()));
            pstmt.setString(5,  consumerUtil.removeAsciNull(cnsmDtl.getHSAAFFIRMIND()));
            pstmt.setString(6,  consumerUtil.removeAsciNull(cnsmDtl.getHSAELECSIGIND()));
            pstmt.setString(7,  consumerUtil.removeAsciNull(cnsmDtl.getHSAWETSIGIND()));
            pstmt.setString(8,  consumerUtil.removeAsciNull(cnsmDtl.getTRANSMETNM()));
            pstmt.setString(9,  consumerUtil.removeAsciNull(cnsmDtl.getUPCTSYSID()));
            pstmt.setInt(10, cnsmDtl.getUPCTPOLID());
            pstmt.setInt(11, cnsmDtl.getUPCTMBRID());
            pstmt.setDate(12, (Date) consumerUtil.dateParser(cnsmDtl.getMBRHIPAACERTDT().toString()));
            pstmt.setString(13,  consumerUtil.removeAsciNull(cnsmDtl.getMBRPRTBIND()));
            pstmt.setString(14,  consumerUtil.removeAsciNull(cnsmDtl.getLTENRLTYPCD()));
            pstmt.setString(15,  consumerUtil.removeAsciNull(cnsmDtl.getFRANCHCD()));
            pstmt.setString(16,  consumerUtil.removeAsciNull(cnsmDtl.getHLTHPGMCD()));
            pstmt.setString(17,  consumerUtil.removeAsciNull(cnsmDtl.getHLTHPLNCD()));
            pstmt.setString(18,  consumerUtil.removeAsciNull(cnsmDtl.getEXSPOTYPCD()));
            pstmt.setString(19,  consumerUtil.removeAsciNull(cnsmDtl.getHLTHCOVTRNSFIND()));
            pstmt.setDate(20, (Date) consumerUtil.dateParser( consumerUtil.removeAsciNull(cnsmDtl.getHLTHCOVTRNSFEFFDT().toString())));
            pstmt.setDate(21, (Date) consumerUtil.dateParser( consumerUtil.removeAsciNull(cnsmDtl.getHLTHCOVTRNSFCANCDT().toString())));
            pstmt.setString(22,  consumerUtil.removeAsciNull(cnsmDtl.getAOTYPCD()));
            pstmt.setString(23,  consumerUtil.removeAsciNull(cnsmDtl.getPCPMEDDIRAPPIND()));
            pstmt.setString(24,  consumerUtil.removeAsciNull(cnsmDtl.getRRCOVCONTYR()));
            pstmt.setDate(25, (Date) consumerUtil.dateParser( consumerUtil.removeAsciNull(cnsmDtl.getDEPNELIGPROOFDT().toString())));
            pstmt.setInt(26, cnsmDtl.getSRCCDBXREFID());
            pstmt.setInt(27, cnsmDtl.getXREFIDPARTNNBR());
            pstmt.setString(28,  consumerUtil.removeAsciNull(cnsmDtl.getUPDTTYPCD()));
            pstmt.setString(29,  consumerUtil.removeAsciNull(cnsmDtl.getRACFID()));
            pstmt.setString(30,  consumerUtil.removeAsciNull(cnsmDtl.getROWUSERID()));
            pstmt.setString(31,  consumerUtil.removeAsciNull(cnsmDtl.getROWSTSCD()));
            pstmt.setTimestamp(32, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmDtl.getSRCTMSTMP().toString())));
            pstmt.setTimestamp(33, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmDtl.getROWTMSTMP().toString())));
            pstmt.setString(34,  consumerUtil.removeAsciNull(cnsmDtl.getCTZNSTSTYPCD()));
            pstmt.setString(35,  consumerUtil.removeAsciNull(cnsmDtl.getHGTNBR()));
            pstmt.setString(36,  consumerUtil.removeAsciNull(cnsmDtl.getWGTNBR()));
            pstmt.setString(37,  consumerUtil.removeAsciNull(cnsmDtl.getEXSPOSBSCRID()));
            pstmt.setString(38,  consumerUtil.removeAsciNull(cnsmDtl.getMNLOVRDTYPCD()));
            pstmt.setString(39,  consumerUtil.removeAsciNull(cnsmDtl.getCMLPRXSTTYPCD()));
            pstmt.setString(40,  consumerUtil.removeAsciNull(cnsmDtl.getCOECD()));
            pstmt.setString(41,  consumerUtil.removeAsciNull(cnsmDtl.getSOCWN()));
            pstmt.setString(42,  consumerUtil.removeAsciNull(cnsmDtl.getCUSTNM()));
            pstmt.setString(43,  consumerUtil.removeAsciNull(cnsmDtl.getCOEDESC()));
            pstmt.setString(44, utilityConfig.getSrcSysId());
            pstmt.setTimestamp(45, consumerUtil.getsysTimeStamp());
            pstmt.setTimestamp(46, consumerUtil.getsysTimeStamp());

            pstmt.setString(47,  consumerUtil.removeAsciNull(cnsmDtl.getHSAAFFIRMIND()));
            pstmt.setString(48,  consumerUtil.removeAsciNull(cnsmDtl.getHSAELECSIGIND()));
            pstmt.setString(49,  consumerUtil.removeAsciNull(cnsmDtl.getHSAWETSIGIND()));
            pstmt.setString(50, consumerUtil.removeAsciNull( cnsmDtl.getTRANSMETNM()));
            pstmt.setString(51,  consumerUtil.removeAsciNull(cnsmDtl.getUPCTSYSID()));
            pstmt.setInt(52, cnsmDtl.getUPCTPOLID());
            pstmt.setInt(53, cnsmDtl.getUPCTMBRID());
            pstmt.setDate(54, (Date) consumerUtil.dateParser( consumerUtil.removeAsciNull(cnsmDtl.getMBRHIPAACERTDT().toString())));
            pstmt.setString(55,  consumerUtil.removeAsciNull(cnsmDtl.getMBRPRTBIND()));
            pstmt.setString(56,  consumerUtil.removeAsciNull(cnsmDtl.getLTENRLTYPCD()));
            pstmt.setString(57,  consumerUtil.removeAsciNull(cnsmDtl.getFRANCHCD()));
            pstmt.setString(58,  consumerUtil.removeAsciNull(cnsmDtl.getHLTHPGMCD()));
            pstmt.setString(59,  consumerUtil.removeAsciNull(cnsmDtl.getHLTHPLNCD()));
            pstmt.setString(60,  consumerUtil.removeAsciNull(cnsmDtl.getEXSPOTYPCD()));
            pstmt.setString(61,  consumerUtil.removeAsciNull(cnsmDtl.getHLTHCOVTRNSFIND()));
            pstmt.setDate(62, (Date) consumerUtil.dateParser( consumerUtil.removeAsciNull(cnsmDtl.getHLTHCOVTRNSFEFFDT().toString())));
            pstmt.setDate(63, (Date) consumerUtil.dateParser( consumerUtil.removeAsciNull(cnsmDtl.getHLTHCOVTRNSFCANCDT().toString())));
            pstmt.setString(64,  consumerUtil.removeAsciNull(cnsmDtl.getAOTYPCD()));
            pstmt.setString(65,  consumerUtil.removeAsciNull(cnsmDtl.getPCPMEDDIRAPPIND()));
            pstmt.setString(66,  consumerUtil.removeAsciNull(cnsmDtl.getRRCOVCONTYR()));
            pstmt.setDate(67, (Date) consumerUtil.dateParser( consumerUtil.removeAsciNull(cnsmDtl.getDEPNELIGPROOFDT().toString())));
            pstmt.setInt(68, cnsmDtl.getCNSMID());
            pstmt.setInt(69, cnsmDtl.getPARTNNBR());
            pstmt.setString(70,  consumerUtil.removeAsciNull(cnsmDtl.getUPDTTYPCD()));
            pstmt.setString(71,  consumerUtil.removeAsciNull(cnsmDtl.getRACFID()));
            pstmt.setString(72,  consumerUtil.removeAsciNull(cnsmDtl.getROWUSERID()));
            pstmt.setString(73,  consumerUtil.removeAsciNull(cnsmDtl.getROWSTSCD()));
            pstmt.setTimestamp(74, consumerUtil.timeStampParser( consumerUtil.removeAsciNull(cnsmDtl.getSRCTMSTMP().toString())));
            pstmt.setTimestamp(75, consumerUtil.timeStampParser( consumerUtil.removeAsciNull(cnsmDtl.getROWTMSTMP().toString())));
            pstmt.setString(76,  consumerUtil.removeAsciNull(cnsmDtl.getCTZNSTSTYPCD()));
            pstmt.setString(77,  consumerUtil.removeAsciNull(cnsmDtl.getHGTNBR()));
            pstmt.setString(78,  consumerUtil.removeAsciNull(cnsmDtl.getWGTNBR()));
            pstmt.setString(79,  consumerUtil.removeAsciNull(cnsmDtl.getEXSPOSBSCRID()));
            pstmt.setString(80,  consumerUtil.removeAsciNull(cnsmDtl.getMNLOVRDTYPCD()));
            pstmt.setString(81,  consumerUtil.removeAsciNull(cnsmDtl.getCMLPRXSTTYPCD()));
            pstmt.setString(82,  consumerUtil.removeAsciNull(cnsmDtl.getCOECD()));
            pstmt.setString(83,  consumerUtil.removeAsciNull(cnsmDtl.getSOCWN()));
            pstmt.setString(84,  consumerUtil.removeAsciNull(cnsmDtl.getCUSTNM()));
            pstmt.setString(85,  consumerUtil.removeAsciNull(cnsmDtl.getCOEDESC()));
            pstmt.setString(86, utilityConfig.getSrcSysId());
            pstmt.setTimestamp(87, consumerUtil.getsysTimeStamp());
                        pstmt.setTimestamp(88,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmDtl.getROWTMSTMP())));


                    }});

        return batch_count[0];
    }

 /*   public int deleteActor() {
        String SQL = "update  WHERE partn_nbr=? and cnsm_id=? and src_cd=? and lgcy_src_id = ?";

        String sql = utilityConfig.getQueryLookup().get("cnsmDtlDeL").replace("<SCHEMA>", utilityConfig.getSchema())


        int affectedrows = 0;

        try (Connection conn = connect();
             PreparedStatement pstmt = conn.prepareStatement(SQL)) {





            affectedrows = pstmt.executeUpdate();

        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
        return affectedrows;
    }
*/

    public int delete(com.optum.exts.cdb.model.key.CNSM_DTL key )throws DataAccessException, SQLException {
        //String SQL = "UPDATE systest.CNSM_DTL SET row_sts_cd = ?  WHERE partn_nbr=? and cnsm_id=? and src_cd=? and lgcy_src_id = ? ";

        String sql = utilityConfig.getQueryLookup().get("cnsmDtlDel").replace("<SCHEMA>", utilityConfig.getSchema());

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
            pstmt.setString(5, consumerUtil.removeAsciNull( key.getSRCCD()));
            pstmt.setString(6,  consumerUtil.removeAsciNull(key.getLGCYSRCID()));

        }});

        return batch_count[0];
    }
}
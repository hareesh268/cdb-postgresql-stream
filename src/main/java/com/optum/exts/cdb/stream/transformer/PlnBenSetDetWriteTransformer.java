package com.optum.exts.cdb.stream.transformer;


import com.optum.exts.cdb.stream.config.UtilityConfig;
import com.optum.exts.cdb.stream.utility.ConsumerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by jjosep68
 */

@Configuration
@ConfigurationProperties("spring.datasource")
public class PlnBenSetDetWriteTransformer {

    private static final Logger log = LoggerFactory.getLogger(PlnBenSetDetWriteTransformer.class);


    @Autowired
    UtilityConfig utilityConfig;
    @Autowired
    ConsumerUtil consumerUtil;
    @Autowired
    private JdbcTemplate jdbcTemplate;


    public int insertData(com.optum.exts.cdb.model.key.PLN_BEN_SET_DET key, com.optum.exts.cdb.model.PLN_BEN_SET_DET plnBenSetDet) throws DataAccessException, SQLException {

        String sql = utilityConfig.getQueryLookup().get("plnBenSetDet").replace("<SCHEMA>", utilityConfig.getSchema());

//        if ((plnBenSetDet.getROWUSERID().trim()).equalsIgnoreCase("LINK/UNLINK") && (plnBenSetDet.getROWSTSCD().trim()).equalsIgnoreCase("D")) {
//            log.info("Skipping Record due to Link/Unlink Delete  :::" + key);
//            return 1;
//        }


        int[] batch_count = jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {

            @Override
            public int getBatchSize() {
                return 1;
            }

            @Override
            public void setValues(PreparedStatement pstmt, int i) throws SQLException {


                pstmt.setString(1, consumerUtil.removeAsciNull(plnBenSetDet.getSRCCD()));
                pstmt.setString(2, consumerUtil.removeAsciNull(plnBenSetDet.getLGCYPOLNBR()));
                pstmt.setString(3, consumerUtil.removeAsciNull(plnBenSetDet.getCOVTYPCD()));
                pstmt.setString(4, consumerUtil.removeAsciNull(plnBenSetDet.getLGCYBENPLNID()));
                pstmt.setDate(5, consumerUtil.dateParser(consumerUtil.removeAsciNull(plnBenSetDet.getPLNBENSETDETEFFDT())));
                pstmt.setDate(6, consumerUtil.dateParser(consumerUtil.removeAsciNull(plnBenSetDet.getPLNBENSETDETCANCDT())));
                pstmt.setString(7, consumerUtil.removeAsciNull(plnBenSetDet.getLGCYPRDTCD()));
                pstmt.setString(8, consumerUtil.removeAsciNull(plnBenSetDet.getLGCYPRDTID()));
                pstmt.setString(9, consumerUtil.removeAsciNull(plnBenSetDet.getFUNDTYPCD()));
                pstmt.setString(10, consumerUtil.removeAsciNull(plnBenSetDet.getSTATEOFISSUECD()));
                pstmt.setString(11, consumerUtil.removeAsciNull(plnBenSetDet.getPROCSTATE()));
                pstmt.setString(12, consumerUtil.removeAsciNull(plnBenSetDet.getUBHCD()));
                pstmt.setString(13, consumerUtil.removeAsciNull(plnBenSetDet.getEAPTYPCD()));
                pstmt.setString(14, consumerUtil.removeAsciNull(plnBenSetDet.getURNCD()));
                pstmt.setString(15, consumerUtil.removeAsciNull(plnBenSetDet.getPRDTSRVCTYPCD()));
                pstmt.setString(16, consumerUtil.removeAsciNull(plnBenSetDet.getHAPLNDSCNTTYPCD()));
                pstmt.setString(17, consumerUtil.removeAsciNull(plnBenSetDet.getDEPNMAXAGENBR()));
                pstmt.setString(18, consumerUtil.removeAsciNull(plnBenSetDet.getDEPNSTDNTAGENBR()));
                pstmt.setString(19, consumerUtil.removeAsciNull(plnBenSetDet.getCOMBLIABTYPCD()));
                pstmt.setString(20, consumerUtil.removeAsciNull(plnBenSetDet.getMEDICATRVLBENIND()));
                pstmt.setDouble(21, Double.valueOf(consumerUtil.removeAsciNull(plnBenSetDet.getPCPCOPAYAMT().toString())));
                pstmt.setDouble(22, Double.valueOf(consumerUtil.removeAsciNull(plnBenSetDet.getSPCLCOPAYAMT().toString())));
                pstmt.setDouble(23, Double.valueOf(consumerUtil.removeAsciNull(plnBenSetDet.getHOSPCOPAYAMT().toString())));
                pstmt.setDouble(24, Double.valueOf(consumerUtil.removeAsciNull(plnBenSetDet.getERCOPAYAMT().toString())));
                pstmt.setDouble(25, Double.valueOf(consumerUtil.removeAsciNull(plnBenSetDet.getRXCOPAYAMT().toString())));
                pstmt.setDouble(26, Double.valueOf(consumerUtil.removeAsciNull(plnBenSetDet.getOFCVSTCOPAYAMT().toString())));
                pstmt.setDouble(27, Double.valueOf(consumerUtil.removeAsciNull(plnBenSetDet.getURGNTCARECOPAYAMT().toString())));
                pstmt.setString(28, consumerUtil.removeAsciNull(plnBenSetDet.getVISNRIDERTYPCD()));
                pstmt.setString(29, consumerUtil.removeAsciNull(plnBenSetDet.getDNTLRIDERCD()));
                pstmt.setDouble(30, Double.valueOf(consumerUtil.removeAsciNull(plnBenSetDet.getBENPLNDEDAMT().toString())));
                pstmt.setDouble(31, Double.valueOf(consumerUtil.removeAsciNull(plnBenSetDet.getBENPLNCOINSAMT().toString())));
                pstmt.setString(32, consumerUtil.removeAsciNull(plnBenSetDet.getPRRCARRTYPCD()));
                pstmt.setString(33, consumerUtil.removeAsciNull(plnBenSetDet.getVSNPLNCD()));
                pstmt.setString(34, consumerUtil.removeAsciNull(plnBenSetDet.getCOPAYPRNTCARDCD()));
                pstmt.setString(35, consumerUtil.removeAsciNull(plnBenSetDet.getNHATYPCD()));
                pstmt.setString(36, consumerUtil.removeAsciNull(plnBenSetDet.getUPDTTYPCD()));
                pstmt.setString(37, consumerUtil.removeAsciNull(plnBenSetDet.getRACFID()));
                pstmt.setString(38, consumerUtil.removeAsciNull(plnBenSetDet.getROWUSERID()));
                pstmt.setString(39, consumerUtil.removeAsciNull(plnBenSetDet.getROWSTSCD()));
                pstmt.setTimestamp(40, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(plnBenSetDet.getSRCTMSTMP())));
                pstmt.setTimestamp(41, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(plnBenSetDet.getROWTMSTMP())));
                pstmt.setString(42, consumerUtil.removeAsciNull(plnBenSetDet.getGLBSLTNSPRDTIND()));
                pstmt.setString(43, consumerUtil.removeAsciNull(plnBenSetDet.getMNTLHLTHRIDERCD()));
                pstmt.setString(44, consumerUtil.removeAsciNull(plnBenSetDet.getEHBIND()));
                pstmt.setString(45, consumerUtil.removeAsciNull(plnBenSetDet.getEHBVSNRIDERCD()));
                pstmt.setString(46, consumerUtil.removeAsciNull(plnBenSetDet.getEHBDNTLRIDERCD()));
                pstmt.setString(47, consumerUtil.removeAsciNull(plnBenSetDet.getDVCNIND()));
                pstmt.setString(48, consumerUtil.removeAsciNull(plnBenSetDet.getMOTIND()));
                pstmt.setString(49, consumerUtil.removeAsciNull(plnBenSetDet.getMOTPARTCD()));
                pstmt.setString(50, utilityConfig.getSrcSysId());  //src_sys_id
                pstmt.setTimestamp(51, consumerUtil.getsysTimeStamp());  //created_dttm
                pstmt.setTimestamp(52, consumerUtil.getsysTimeStamp()); //updt_dttm
                //  pstmt.setString(53, consumerUtil.removeAsciNull(plnBenSetDet.getPLNIND()));
//                pstmt.setString(53,"N"); // pln_ind

               // pstmt.setDate(53, consumerUtil.dateParser(plnBenSetDet.getPLNBENSETDETCANCDT()));
                pstmt.setString(53, consumerUtil.removeAsciNull(plnBenSetDet.getLGCYPRDTCD()));
                pstmt.setString(54, consumerUtil.removeAsciNull(plnBenSetDet.getLGCYPRDTID()));
                pstmt.setString(55, consumerUtil.removeAsciNull(plnBenSetDet.getFUNDTYPCD()));
                pstmt.setString(56, consumerUtil.removeAsciNull(plnBenSetDet.getSTATEOFISSUECD()));
                pstmt.setString(57, consumerUtil.removeAsciNull(plnBenSetDet.getPROCSTATE()));
                pstmt.setString(58, consumerUtil.removeAsciNull(plnBenSetDet.getUBHCD()));
                pstmt.setString(59, consumerUtil.removeAsciNull(plnBenSetDet.getEAPTYPCD()));
                pstmt.setString(60, consumerUtil.removeAsciNull(plnBenSetDet.getURNCD()));
                pstmt.setString(61, consumerUtil.removeAsciNull(plnBenSetDet.getPRDTSRVCTYPCD()));
                pstmt.setString(62, consumerUtil.removeAsciNull(plnBenSetDet.getHAPLNDSCNTTYPCD()));
                pstmt.setString(63, consumerUtil.removeAsciNull(plnBenSetDet.getDEPNMAXAGENBR()));
                pstmt.setString(64, consumerUtil.removeAsciNull(plnBenSetDet.getDEPNSTDNTAGENBR()));
                pstmt.setString(65, consumerUtil.removeAsciNull(plnBenSetDet.getCOMBLIABTYPCD()));
                pstmt.setString(66, consumerUtil.removeAsciNull(plnBenSetDet.getMEDICATRVLBENIND()));
                pstmt.setDouble(67, Double.valueOf(consumerUtil.removeAsciNull(plnBenSetDet.getPCPCOPAYAMT())));
                pstmt.setDouble(68, Double.valueOf(consumerUtil.removeAsciNull(plnBenSetDet.getSPCLCOPAYAMT())));
                pstmt.setDouble(69, Double.valueOf(consumerUtil.removeAsciNull(plnBenSetDet.getHOSPCOPAYAMT())));
                pstmt.setDouble(70, Double.valueOf(consumerUtil.removeAsciNull(plnBenSetDet.getERCOPAYAMT())));
                pstmt.setDouble(71, Double.valueOf(consumerUtil.removeAsciNull(plnBenSetDet.getRXCOPAYAMT())));
                pstmt.setDouble(72, Double.valueOf(consumerUtil.removeAsciNull(plnBenSetDet.getOFCVSTCOPAYAMT())));
                pstmt.setDouble(73, Double.valueOf(consumerUtil.removeAsciNull(plnBenSetDet.getURGNTCARECOPAYAMT())));
                pstmt.setString(74, consumerUtil.removeAsciNull(plnBenSetDet.getVISNRIDERTYPCD()));
                pstmt.setString(75, consumerUtil.removeAsciNull(plnBenSetDet.getDNTLRIDERCD()));
                pstmt.setDouble(76, Double.valueOf(consumerUtil.removeAsciNull(plnBenSetDet.getBENPLNDEDAMT())));
                pstmt.setDouble(77, Double.valueOf(consumerUtil.removeAsciNull(plnBenSetDet.getBENPLNCOINSAMT())));
                pstmt.setString(78, consumerUtil.removeAsciNull(plnBenSetDet.getPRRCARRTYPCD()));
                pstmt.setString(79, consumerUtil.removeAsciNull(plnBenSetDet.getVSNPLNCD()));
                pstmt.setString(80, consumerUtil.removeAsciNull(plnBenSetDet.getCOPAYPRNTCARDCD()));
                pstmt.setString(81, consumerUtil.removeAsciNull(plnBenSetDet.getNHATYPCD()));
                pstmt.setString(82, consumerUtil.removeAsciNull(plnBenSetDet.getUPDTTYPCD()));
                pstmt.setString(83, consumerUtil.removeAsciNull(plnBenSetDet.getRACFID()));
                pstmt.setString(84, consumerUtil.removeAsciNull(plnBenSetDet.getROWUSERID()));
                pstmt.setString(85, consumerUtil.removeAsciNull(plnBenSetDet.getROWSTSCD()));
                pstmt.setTimestamp(86, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(plnBenSetDet.getSRCTMSTMP())));
                pstmt.setTimestamp(87, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(plnBenSetDet.getROWTMSTMP())));
                pstmt.setString(88, consumerUtil.removeAsciNull(plnBenSetDet.getGLBSLTNSPRDTIND()));
                pstmt.setString(89, consumerUtil.removeAsciNull(plnBenSetDet.getMNTLHLTHRIDERCD()));
                pstmt.setString(90, consumerUtil.removeAsciNull(plnBenSetDet.getEHBIND()));
                pstmt.setString(91, consumerUtil.removeAsciNull(plnBenSetDet.getEHBVSNRIDERCD()));
                pstmt.setString(92, consumerUtil.removeAsciNull(plnBenSetDet.getEHBDNTLRIDERCD()));
                pstmt.setString(93, consumerUtil.removeAsciNull(plnBenSetDet.getDVCNIND()));
                pstmt.setString(94, consumerUtil.removeAsciNull(plnBenSetDet.getMOTIND()));
                pstmt.setString(95, consumerUtil.removeAsciNull(plnBenSetDet.getMOTPARTCD()));
                pstmt.setString(96, utilityConfig.getSrcSysId());  //src_sys_id
                pstmt.setTimestamp(97, consumerUtil.getsysTimeStamp()); //updt_dttm
                pstmt.setTimestamp(98,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(plnBenSetDet.getROWTMSTMP())));

                //  pstmt.setString(100, consumerUtil.removeAsciNull(plnBenSetDet.getPLNIND()));
//                pstmt.setString(100,"N"); // pln_ind

            }
        });


        return batch_count[0];
    }

    public int delete(com.optum.exts.cdb.model.key.PLN_BEN_SET_DET key) throws DataAccessException, SQLException {

        String sql = utilityConfig.getQueryLookup().get("plnBenSetDetDel").replace("<SCHEMA>", utilityConfig.getSchema());

        int[] batch_count = jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {

            @Override
            public int getBatchSize() {
                return 1;
            }

            @Override
            public void setValues(PreparedStatement pstmt, int i) throws SQLException {
                pstmt.setString(1, utilityConfig.getPhysicalDelValue());
                pstmt.setTimestamp(2, consumerUtil.getsysTimeStamp());
                pstmt.setString(3, consumerUtil.removeAsciNull(key.getSRCCD()));
                pstmt.setString(4, consumerUtil.removeAsciNull(key.getLGCYPOLNBR()));
                pstmt.setString(5, consumerUtil.removeAsciNull(key.getCOVTYPCD()));
                pstmt.setString(6, consumerUtil.removeAsciNull(key.getLGCYBENPLNID()));
                pstmt.setDate(7, consumerUtil.dateParser(consumerUtil.removeAsciNull(key.getPLNBENSETDETEFFDT())));
                pstmt.setDate(8, consumerUtil.dateParser(consumerUtil.removeAsciNull(key.getPLNBENSETDETCANCDT())));
            }
        });


        return batch_count[0];
    }


}
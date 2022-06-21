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
public class LCovPrdtWriteTransformer {

    private static final Logger log = LoggerFactory.getLogger(LCovPrdtWriteTransformer.class);


    @Autowired
    UtilityConfig utilityConfig;
    @Autowired
    ConsumerUtil consumerUtil;
    @Autowired
    JdbcTemplate jdbcTemplate;






    public int insertData(com.optum.exts.cdb.model.key.L_COV_PRDT_DT key, com.optum.exts.cdb.model.L_COV_PRDT_DT lCovPrdt)throws DataAccessException, SQLException {

//log.info("L_COV_PRDT_DT processing key "+key.toString());
        if((lCovPrdt.getROWUSERID().toString().trim()).equalsIgnoreCase("LINK/UNLINK") && (lCovPrdt.getROWSTSCD().toString().trim()).equalsIgnoreCase("D") ) {

            log.info("Skipping Record due to Link/Unlink Delete  :::" + key);
            return 0;
        }

        String sql = utilityConfig.getQueryLookup().get("lCovPrdtDt").replace("<SCHEMA>", utilityConfig.getSchema());
        ;


       // String id = "";

        int[] batch_count= jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {

            @Override
            public int getBatchSize() {
                return 1;
            }

            @Override
            public void setValues(PreparedStatement pstmt, int i) throws SQLException {



                pstmt.setDate(1, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lCovPrdt.getCOVEFFDT().toString())));
            pstmt.setDate(2, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lCovPrdt.getCOVCANCDT().toString())));
            pstmt.setDate(3, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lCovPrdt.getCOVPDTHRUDT().toString())));
            pstmt.setDate(4, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lCovPrdt.getEBILLDT().toString())));
            pstmt.setDate(5, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lCovPrdt.getRETROELIGRECVDT().toString())));
            pstmt.setDate(6, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lCovPrdt.getRETROORIGCOVEFFDT().toString())));
            pstmt.setDate(7, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lCovPrdt.getRETROORIGCOVCANCDT().toString())));
            pstmt.setDate(8, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lCovPrdt.getCOBRAEFFDT().toString())));
            pstmt.setDate(9, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lCovPrdt.getELIGGRCPRDTHRUDT().toString())));
            pstmt.setDate(10, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lCovPrdt.getLSTPREMPDDT().toString())));
            pstmt.setDate(11, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lCovPrdt.getPOLRENDT().toString())));
            pstmt.setInt(12, lCovPrdt.getPARTNNBR());
            pstmt.setInt(13, lCovPrdt.getXREFIDPARTNNBR());
            pstmt.setInt(14, lCovPrdt.getCNSMID());
            pstmt.setInt(15, lCovPrdt.getPRFLID());
            pstmt.setInt(16, lCovPrdt.getSRCCDBXREFID());
            pstmt.setDouble(17, Double.valueOf(lCovPrdt.getCOSPNLNBR()));
            pstmt.setTimestamp(18, consumerUtil.timeStampParser(lCovPrdt.getSRCTMSTMP().toString()));
            pstmt.setTimestamp(19, consumerUtil.timeStampParser(lCovPrdt.getROWTMSTMP().toString()));
            pstmt.setTimestamp(20, consumerUtil.getsysTimeStamp());
            pstmt.setTimestamp(21, consumerUtil.getsysTimeStamp());
            pstmt.setString(22, consumerUtil.removeAsciNull(lCovPrdt.getSRCCD()));
            pstmt.setString(23, consumerUtil.removeAsciNull(lCovPrdt.getLGCYPOLNBR()));
            pstmt.setString(24, consumerUtil.removeAsciNull(lCovPrdt.getLGCYSRCID()));
            pstmt.setString(25, consumerUtil.removeAsciNull(lCovPrdt.getCOVTYPCD()));
            pstmt.setString(26, consumerUtil.removeAsciNull(lCovPrdt.getCOSDIVCD()));
            pstmt.setString(27, consumerUtil.removeAsciNull(lCovPrdt.getMKTTYPCD()));
            pstmt.setString(28, consumerUtil.removeAsciNull(lCovPrdt.getCOSGRPNBR()));
            pstmt.setString(29, consumerUtil.removeAsciNull(lCovPrdt.getLGCYPRDTTYPCD()));
            pstmt.setString(30, consumerUtil.removeAsciNull(lCovPrdt.getLGCYPRDTCD()));
            pstmt.setString(31, consumerUtil.removeAsciNull(lCovPrdt.getCOVLVLTYPCD()));
            pstmt.setString(32, consumerUtil.removeAsciNull(lCovPrdt.getSHRARNGCD()));
            pstmt.setString(33, consumerUtil.removeAsciNull(lCovPrdt.getSHRARNGOBLIGCD()));
            pstmt.setString(34, consumerUtil.removeAsciNull(lCovPrdt.getLGCYPLNVARCD()));
            pstmt.setString(35, consumerUtil.removeAsciNull(lCovPrdt.getLGCYRPTCD()));
            pstmt.setString(36, consumerUtil.removeAsciNull(lCovPrdt.getPRDTSRVCTYPCD()));
            pstmt.setString(37, consumerUtil.removeAsciNull(lCovPrdt.getEESTSTYPCD()));
            pstmt.setString(38, consumerUtil.removeAsciNull(lCovPrdt.getGOVTPGMTYPCD()));
            pstmt.setString(39, consumerUtil.removeAsciNull(lCovPrdt.getCLMSYSTYPCD()));
            pstmt.setString(40, consumerUtil.removeAsciNull(lCovPrdt.getELIGSYSTYPCD()));
            pstmt.setString(41, consumerUtil.removeAsciNull(lCovPrdt.getCESGRPNBR()));
            pstmt.setString(42, consumerUtil.removeAsciNull(lCovPrdt.getMKTSITECD()));
            pstmt.setString(43, consumerUtil.removeAsciNull(lCovPrdt.getROWSTSCD()));
            pstmt.setString(44, consumerUtil.removeAsciNull(lCovPrdt.getMEDICATRVLBENIND()));
            pstmt.setString(45, consumerUtil.removeAsciNull(lCovPrdt.getROWUSERID()));
            pstmt.setString(46, consumerUtil.removeAsciNull(lCovPrdt.getSECTYPCD()));
            pstmt.setString(47, consumerUtil.removeAsciNull(lCovPrdt.getCANCELRSNTYPCD()));
            pstmt.setString(48, consumerUtil.removeAsciNull(lCovPrdt.getCOVPDTHRURSNCD()));
            pstmt.setString(49, consumerUtil.removeAsciNull(lCovPrdt.getLISTBILLTYPCD()));
            pstmt.setString(50, consumerUtil.removeAsciNull(lCovPrdt.getBILLINGSUFXCD()));
            pstmt.setString(51, consumerUtil.removeAsciNull(lCovPrdt.getBILLINGSUBGRPNBR()));
            pstmt.setString(52, consumerUtil.removeAsciNull(lCovPrdt.getRETRODAYS()));
            pstmt.setString(53, consumerUtil.removeAsciNull(lCovPrdt.getRETROTYPCD()));
            pstmt.setString(54, consumerUtil.removeAsciNull(lCovPrdt.getRETROOVRDTYPCD()));
            pstmt.setString(55, consumerUtil.removeAsciNull(lCovPrdt.getTOPSCOVLVLTYPCD()));
            pstmt.setString(56, consumerUtil.removeAsciNull(lCovPrdt.getLGCYBENPLNID()));
            pstmt.setString(57, consumerUtil.removeAsciNull(lCovPrdt.getLGCYPRDTID()));
            pstmt.setString(58, consumerUtil.removeAsciNull(lCovPrdt.getRRBENGRPNBR()));
            pstmt.setString(59, consumerUtil.removeAsciNull(lCovPrdt.getRRBENGRPCHOCD()));
            pstmt.setString(60, consumerUtil.removeAsciNull(lCovPrdt.getRRBRCD()));
            pstmt.setString(61, consumerUtil.removeAsciNull(lCovPrdt.getRRUNCD()));
            pstmt.setString(62, consumerUtil.removeAsciNull(lCovPrdt.getRROPTOUTPLANIND()));
            pstmt.setString(63, consumerUtil.removeAsciNull(lCovPrdt.getUPDTTYPCD()));
            pstmt.setString(64, consumerUtil.removeAsciNull(lCovPrdt.getRACFID()));
            pstmt.setString(65, consumerUtil.removeAsciNull(lCovPrdt.getPRRCOVMO()));
            pstmt.setString(66, consumerUtil.removeAsciNull(lCovPrdt.getFUNDTYPCD()));
            pstmt.setString(67, consumerUtil.removeAsciNull(lCovPrdt.getSTATEOFISSUECD()));
            pstmt.setString(68, consumerUtil.removeAsciNull(lCovPrdt.getCOBRAMO()));
            pstmt.setString(69, consumerUtil.removeAsciNull(lCovPrdt.getCOBRAQUALEVNTCD()));
            pstmt.setString(70, consumerUtil.removeAsciNull(lCovPrdt.getGRNDFATHEREDPOLIND()));
            pstmt.setString(71, consumerUtil.removeAsciNull(lCovPrdt.getDERIVCOVIND()));
            pstmt.setString(72, consumerUtil.removeAsciNull(lCovPrdt.getCNSMLGLENTYNM()));
            pstmt.setString(73, consumerUtil.removeAsciNull(lCovPrdt.getINDVGRPTYPCD()));
            pstmt.setString(74, consumerUtil.removeAsciNull(lCovPrdt.getSRCCOVMNTTYPCD()));
            pstmt.setString(75, consumerUtil.removeAsciNull(lCovPrdt.getPBPCD()));
            pstmt.setString(76, consumerUtil.removeAsciNull(lCovPrdt.getHCNTRCTID()));
            pstmt.setString(77, consumerUtil.removeAsciNull(lCovPrdt.getRISKTYPCD()));
            pstmt.setString(78, consumerUtil.removeAsciNull(lCovPrdt.getBILTYPCD()));
            pstmt.setString(79, consumerUtil.removeAsciNull(lCovPrdt.getRATECOVTYPCD()));
            pstmt.setString(80, consumerUtil.removeAsciNull(lCovPrdt.getPLANCD()));
            pstmt.setString(81, consumerUtil.removeAsciNull(lCovPrdt.getSEGID()));
            pstmt.setString(82, utilityConfig.getSrcSysId());


            pstmt.setDate(83, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lCovPrdt.getCOVCANCDT().toString())));
            pstmt.setDate(84, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lCovPrdt.getCOVPDTHRUDT().toString())));
            pstmt.setDate(85, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lCovPrdt.getEBILLDT().toString())));
            pstmt.setDate(86, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lCovPrdt.getRETROELIGRECVDT().toString())));
            pstmt.setDate(87, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lCovPrdt.getRETROORIGCOVEFFDT().toString())));
            pstmt.setDate(88, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lCovPrdt.getRETROORIGCOVCANCDT().toString())));
            pstmt.setDate(89, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lCovPrdt.getCOBRAEFFDT().toString())));
            pstmt.setDate(90, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lCovPrdt.getELIGGRCPRDTHRUDT().toString())));
            pstmt.setDate(91, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lCovPrdt.getLSTPREMPDDT().toString())));
            pstmt.setDate(92, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lCovPrdt.getPOLRENDT().toString())));
            pstmt.setInt(93, lCovPrdt.getPARTNNBR());
            pstmt.setInt(94, lCovPrdt.getPRFLID());
            pstmt.setInt(95, lCovPrdt.getCNSMID());
            pstmt.setDouble(96, Double.valueOf(lCovPrdt.getCOSPNLNBR()));
            pstmt.setTimestamp(97, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(lCovPrdt.getSRCTMSTMP().toString())));
            pstmt.setTimestamp(98, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(lCovPrdt.getROWTMSTMP().toString())));
            pstmt.setTimestamp(99, consumerUtil.getsysTimeStamp());
            pstmt.setString(100, consumerUtil.removeAsciNull(lCovPrdt.getCOSDIVCD()));
            pstmt.setString(101, consumerUtil.removeAsciNull(lCovPrdt.getMKTTYPCD()));
            pstmt.setString(102, consumerUtil.removeAsciNull(lCovPrdt.getCOSGRPNBR()));
            pstmt.setString(103, consumerUtil.removeAsciNull(lCovPrdt.getLGCYPRDTTYPCD()));
            pstmt.setString(104, consumerUtil.removeAsciNull(lCovPrdt.getLGCYPRDTCD()));
            pstmt.setString(105, consumerUtil.removeAsciNull(lCovPrdt.getCOVLVLTYPCD()));
            pstmt.setString(106, consumerUtil.removeAsciNull(lCovPrdt.getSHRARNGCD()));
            pstmt.setString(107, consumerUtil.removeAsciNull(lCovPrdt.getSHRARNGOBLIGCD()));
            pstmt.setString(108, consumerUtil.removeAsciNull(lCovPrdt.getLGCYPLNVARCD()));
            pstmt.setString(109, consumerUtil.removeAsciNull(lCovPrdt.getLGCYRPTCD()));
            pstmt.setString(110, consumerUtil.removeAsciNull(lCovPrdt.getPRDTSRVCTYPCD()));
            pstmt.setString(111, consumerUtil.removeAsciNull(lCovPrdt.getEESTSTYPCD()));
            pstmt.setString(112, consumerUtil.removeAsciNull(lCovPrdt.getGOVTPGMTYPCD()));
            pstmt.setString(113, consumerUtil.removeAsciNull(lCovPrdt.getCLMSYSTYPCD()));
            pstmt.setString(114, consumerUtil.removeAsciNull(lCovPrdt.getELIGSYSTYPCD()));
            pstmt.setString(115, consumerUtil.removeAsciNull(lCovPrdt.getCESGRPNBR()));
            pstmt.setString(116, consumerUtil.removeAsciNull(lCovPrdt.getMKTSITECD()));
            pstmt.setString(117, consumerUtil.removeAsciNull(lCovPrdt.getROWSTSCD()));
            pstmt.setString(118, consumerUtil.removeAsciNull(lCovPrdt.getMEDICATRVLBENIND()));
            pstmt.setString(119, consumerUtil.removeAsciNull(lCovPrdt.getROWUSERID()));
            pstmt.setString(120, consumerUtil.removeAsciNull(lCovPrdt.getSECTYPCD()));
            pstmt.setString(121, consumerUtil.removeAsciNull(lCovPrdt.getCANCELRSNTYPCD()));
            pstmt.setString(122, consumerUtil.removeAsciNull(lCovPrdt.getCOVPDTHRURSNCD()));
            pstmt.setString(123, consumerUtil.removeAsciNull(lCovPrdt.getLISTBILLTYPCD()));
            pstmt.setString(124, consumerUtil.removeAsciNull(lCovPrdt.getBILLINGSUFXCD()));
            pstmt.setString(125, consumerUtil.removeAsciNull(lCovPrdt.getBILLINGSUBGRPNBR()));
            pstmt.setString(126, consumerUtil.removeAsciNull(lCovPrdt.getRETRODAYS()));
            pstmt.setString(127, consumerUtil.removeAsciNull(lCovPrdt.getRETROTYPCD()));
            pstmt.setString(128, consumerUtil.removeAsciNull(lCovPrdt.getRETROOVRDTYPCD()));
            pstmt.setString(129, consumerUtil.removeAsciNull(lCovPrdt.getTOPSCOVLVLTYPCD()));
            pstmt.setString(130, consumerUtil.removeAsciNull(lCovPrdt.getLGCYBENPLNID()));
            pstmt.setString(131, consumerUtil.removeAsciNull(lCovPrdt.getLGCYPRDTID()));
            pstmt.setString(132, consumerUtil.removeAsciNull(lCovPrdt.getRRBENGRPNBR()));
            pstmt.setString(133, consumerUtil.removeAsciNull(lCovPrdt.getRRBENGRPCHOCD()));
            pstmt.setString(134, consumerUtil.removeAsciNull(lCovPrdt.getRRBRCD()));
            pstmt.setString(135, consumerUtil.removeAsciNull(lCovPrdt.getRRUNCD()));
            pstmt.setString(136, consumerUtil.removeAsciNull(lCovPrdt.getRROPTOUTPLANIND()));
            pstmt.setString(137, consumerUtil.removeAsciNull(lCovPrdt.getUPDTTYPCD()));
            pstmt.setString(138, consumerUtil.removeAsciNull(lCovPrdt.getRACFID()));
            pstmt.setString(139, consumerUtil.removeAsciNull(lCovPrdt.getPRRCOVMO()));
            pstmt.setString(140, consumerUtil.removeAsciNull(lCovPrdt.getFUNDTYPCD()));
            pstmt.setString(141, consumerUtil.removeAsciNull(lCovPrdt.getSTATEOFISSUECD()));
            pstmt.setString(142, consumerUtil.removeAsciNull(lCovPrdt.getCOBRAMO()));
            pstmt.setString(143, consumerUtil.removeAsciNull(lCovPrdt.getCOBRAQUALEVNTCD()));
            pstmt.setString(144, consumerUtil.removeAsciNull(lCovPrdt.getGRNDFATHEREDPOLIND()));
            pstmt.setString(145, consumerUtil.removeAsciNull(lCovPrdt.getDERIVCOVIND()));
            pstmt.setString(146, consumerUtil.removeAsciNull(lCovPrdt.getCNSMLGLENTYNM()));
            pstmt.setString(147, consumerUtil.removeAsciNull(lCovPrdt.getINDVGRPTYPCD()));
            pstmt.setString(148, consumerUtil.removeAsciNull(lCovPrdt.getSRCCOVMNTTYPCD()));
            pstmt.setString(149, consumerUtil.removeAsciNull(lCovPrdt.getPBPCD()));
            pstmt.setString(150, consumerUtil.removeAsciNull(lCovPrdt.getHCNTRCTID()));
            pstmt.setString(151, consumerUtil.removeAsciNull(lCovPrdt.getRISKTYPCD()));
            pstmt.setString(152, consumerUtil.removeAsciNull(lCovPrdt.getBILTYPCD()));
            pstmt.setString(153, consumerUtil.removeAsciNull(lCovPrdt.getRATECOVTYPCD()));
            pstmt.setString(154, consumerUtil.removeAsciNull(lCovPrdt.getPLANCD()));
            pstmt.setString(155, consumerUtil.removeAsciNull(lCovPrdt.getSEGID()));
            pstmt.setString(156, utilityConfig.getSrcSysId());
                pstmt.setTimestamp(157,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(lCovPrdt.getROWTMSTMP())));


            }});



        return batch_count[0];
    }


    public int delete(com.optum.exts.cdb.model.key.L_COV_PRDT_DT key ) throws DataAccessException, SQLException{
        //String SQL = "UPDATE systest.CNSM_DTL SET row_sts_cd = ?  WHERE partn_nbr=? and cnsm_id=? and src_cd=? and lgcy_src_id = ? ";

        //String sql1 = utilityConfig.getQueryLookup().get("lCovPrdtDtDel").replace("<SCHEMA>", utilityConfig.getSchema());

        int affectedrows = 0;
        String sql = utilityConfig.getQueryLookup().get("lCovPrdtDtDel").replace("<SCHEMA>", utilityConfig.getSchema());


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
            pstmt.setString(7, consumerUtil.removeAsciNull(key.getCOVEFFDT()));
            pstmt.setString(8, consumerUtil.removeAsciNull(key.getLGCYPOLNBR()));
            pstmt.setString(9, consumerUtil.removeAsciNull(key.getCOVTYPCD()));

            //affectedrows = pstmt.executeUpdate();
            }});



        return batch_count[0];
    }
}
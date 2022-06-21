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
public class LLfDisPrdtDtWriteTransformer {

    private static final Logger log = LoggerFactory.getLogger(LLfDisPrdtDtWriteTransformer.class);

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




    public int insertData(com.optum.exts.cdb.model.key.L_LF_DIS_PRDT_DT key, com.optum.exts.cdb.model.L_LF_DIS_PRDT_DT lLfDisPrdtDt) throws DataAccessException, SQLException{

        if((lLfDisPrdtDt.getROWUSERID().toString().trim()).equalsIgnoreCase("LINK/UNLINK") && (lLfDisPrdtDt.getROWSTSCD().toString().trim()).equalsIgnoreCase("D") ) {

            log.info("Skipping Record due to Link/Unlink Delete  :::" + key);
            return 0;
        }
        String sql = utilityConfig.getQueryLookup().get("lIfDisPrdtDt").replace("<SCHEMA>", utilityConfig.getSchema());
        ;



        int[] batch_count= jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {

            @Override
            public int getBatchSize() {
                return 1;
            }

            @Override
            public void setValues(PreparedStatement pstmt, int i) throws SQLException {
            pstmt.setInt(1, lLfDisPrdtDt.getPARTNNBR());
            pstmt.setString(2, consumerUtil.removeAsciNull(lLfDisPrdtDt.getSRCCD()));
            pstmt.setInt(3, lLfDisPrdtDt.getCNSMID());
            pstmt.setString(4, consumerUtil.removeAsciNull(lLfDisPrdtDt.getLGCYPOLNBR()));
            pstmt.setString(5, consumerUtil.removeAsciNull(lLfDisPrdtDt.getLGCYSRCID()));
            pstmt.setString(6, consumerUtil.removeAsciNull(lLfDisPrdtDt.getCOVTYPCD()));
            pstmt.setString(7, consumerUtil.removeAsciNull(lLfDisPrdtDt.getLGCYPRDTTYPCD()));
            pstmt.setDate(8, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lLfDisPrdtDt.getCOVEFFDT().toString())));
            pstmt.setDate(9, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lLfDisPrdtDt.getCOVCANCDT().toString())));
            pstmt.setString(10, consumerUtil.removeAsciNull(lLfDisPrdtDt.getEESTSTYPCD()));
            pstmt.setString(11, consumerUtil.removeAsciNull(lLfDisPrdtDt.getELIGSYSTYPCD()));
            pstmt.setString(12, consumerUtil.removeAsciNull(lLfDisPrdtDt.getCLMSYSTYPCD()));
            pstmt.setString(13, consumerUtil.removeAsciNull(lLfDisPrdtDt.getROWSTSCD()));
            pstmt.setTimestamp(14, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(lLfDisPrdtDt.getSRCTMSTMP().toString())));
            pstmt.setTimestamp(15, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(lLfDisPrdtDt.getROWTMSTMP().toString())));
            pstmt.setInt(16, lLfDisPrdtDt.getPRFLID());
            pstmt.setString(17, consumerUtil.removeAsciNull(lLfDisPrdtDt.getROWUSERID()));
            pstmt.setString(18, consumerUtil.removeAsciNull(lLfDisPrdtDt.getSECTYPCD()));
            pstmt.setInt(19, lLfDisPrdtDt.getSRCCDBXREFID());
            pstmt.setInt(20, lLfDisPrdtDt.getXREFIDPARTNNBR());
            pstmt.setString(21, consumerUtil.removeAsciNull(lLfDisPrdtDt.getPLANCD()));
            pstmt.setDouble(22, Double.valueOf(consumerUtil.removeAsciNull(lLfDisPrdtDt.getPRDTPREMAMT())));
            pstmt.setDouble(23, Double.valueOf(consumerUtil.removeAsciNull(lLfDisPrdtDt.getINSDAMT())));
            pstmt.setDouble(24, Double.valueOf(consumerUtil.removeAsciNull(lLfDisPrdtDt.getAPRVAMT())));
            pstmt.setString(25, consumerUtil.removeAsciNull(lLfDisPrdtDt.getENTRANTSTSCD()));
            pstmt.setString(26, consumerUtil.removeAsciNull(lLfDisPrdtDt.getEOISTSCD()));
            pstmt.setDate(27, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lLfDisPrdtDt.getEOISTSDT().toString())));
            pstmt.setString(28, consumerUtil.removeAsciNull(lLfDisPrdtDt.getWOPCD()));
            pstmt.setDate(29, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lLfDisPrdtDt.getWOPEFFDT().toString())));
            pstmt.setDate(30, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lLfDisPrdtDt.getWOPCANCDT().toString())));
            pstmt.setString(31, consumerUtil.removeAsciNull(lLfDisPrdtDt.getACCLDTHCD()));
            pstmt.setDouble(32, Double.valueOf(consumerUtil.removeAsciNull(lLfDisPrdtDt.getACCLDTHPAYOAMT())));
            pstmt.setDate(33, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lLfDisPrdtDt.getACCLDTHEFFDT().toString())));
            pstmt.setDouble(34, Double.valueOf(consumerUtil.removeAsciNull(lLfDisPrdtDt.getREQAMT())));
            pstmt.setString(35, consumerUtil.removeAsciNull(lLfDisPrdtDt.getCOVSTSCD()));
            pstmt.setDouble(36, Double.valueOf(consumerUtil.removeAsciNull(lLfDisPrdtDt.getSALBENMULTFCT())));
            pstmt.setDouble(37, Double.valueOf(consumerUtil.removeAsciNull(lLfDisPrdtDt.getAGERDUCAPPLPCT())));
            pstmt.setInt(38, lLfDisPrdtDt.getRDUCAPPLAGENBR());
            pstmt.setString(39, consumerUtil.removeAsciNull(lLfDisPrdtDt.getCANCELRSNTYPCD()));
            pstmt.setDate(40, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lLfDisPrdtDt.getCOVPDTHRUDT().toString())));
            pstmt.setString(41, consumerUtil.removeAsciNull(lLfDisPrdtDt.getCOVPDTHRURSNCD()));
            pstmt.setString(42, consumerUtil.removeAsciNull(lLfDisPrdtDt.getLISTBILLTYPCD()));
            pstmt.setString(43, consumerUtil.removeAsciNull(lLfDisPrdtDt.getBILLINGSUFXCD()));
            pstmt.setString(44, consumerUtil.removeAsciNull(lLfDisPrdtDt.getBILLINGSUBGRPNBR()));
            pstmt.setDate(45, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lLfDisPrdtDt.getEBILLDT().toString())));
            pstmt.setString(46, consumerUtil.removeAsciNull(lLfDisPrdtDt.getLGCYBENPLNID()));
            pstmt.setString(47, consumerUtil.removeAsciNull(lLfDisPrdtDt.getLGCYPLNVARCD()));
            pstmt.setString(48, consumerUtil.removeAsciNull(lLfDisPrdtDt.getLGCYRPTCD()));
            pstmt.setString(49, consumerUtil.removeAsciNull(lLfDisPrdtDt.getUPDTTYPCD()));
            pstmt.setString(50, consumerUtil.removeAsciNull(lLfDisPrdtDt.getRACFID()));
            pstmt.setString(51, consumerUtil.removeAsciNull(lLfDisPrdtDt.getCESGRPNBR()));
            pstmt.setString(52, consumerUtil.removeAsciNull(lLfDisPrdtDt.getFUNDTYPCD()));
            pstmt.setString(53, consumerUtil.removeAsciNull(lLfDisPrdtDt.getSTATEOFISSUECD()));
            pstmt.setString(54, consumerUtil.removeAsciNull(lLfDisPrdtDt.getGRNDFATHEREDPOLIND()));
            pstmt.setString(55, consumerUtil.removeAsciNull(lLfDisPrdtDt.getDERIVCOVIND()));
            pstmt.setString(56, consumerUtil.removeAsciNull(lLfDisPrdtDt.getCNSMLGLENTYNM()));
            pstmt.setString(57, consumerUtil.removeAsciNull(lLfDisPrdtDt.getINDVGRPTYPCD()));
            pstmt.setString(58, consumerUtil.removeAsciNull(lLfDisPrdtDt.getBILTYPCD()));
            pstmt.setString(59, consumerUtil.removeAsciNull(lLfDisPrdtDt.getRATECOVTYPCD()));
            pstmt.setDate(60, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lLfDisPrdtDt.getPOLRENDT().toString())));
            pstmt.setString(61, utilityConfig.getSrcSysId());
            pstmt.setTimestamp(62, consumerUtil.getsysTimeStamp());
            pstmt.setTimestamp(63, consumerUtil.getsysTimeStamp());

            pstmt.setDate(64, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lLfDisPrdtDt.getCOVCANCDT().toString())));
            pstmt.setString(65, consumerUtil.removeAsciNull(lLfDisPrdtDt.getEESTSTYPCD()));
            pstmt.setString(66, consumerUtil.removeAsciNull(lLfDisPrdtDt.getELIGSYSTYPCD()));
            pstmt.setString(67, consumerUtil.removeAsciNull(lLfDisPrdtDt.getCLMSYSTYPCD()));
            pstmt.setString(68, consumerUtil.removeAsciNull(lLfDisPrdtDt.getROWSTSCD()));
            pstmt.setTimestamp(69, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(lLfDisPrdtDt.getSRCTMSTMP().toString())));
            pstmt.setTimestamp(70, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(lLfDisPrdtDt.getROWTMSTMP().toString())));
            pstmt.setInt(71, lLfDisPrdtDt.getPRFLID());
            pstmt.setString(72, consumerUtil.removeAsciNull(lLfDisPrdtDt.getROWUSERID()));
            pstmt.setString(73, consumerUtil.removeAsciNull(lLfDisPrdtDt.getSECTYPCD()));
            pstmt.setInt(74, lLfDisPrdtDt.getCNSMID());
            pstmt.setInt(75, lLfDisPrdtDt.getPARTNNBR());
            pstmt.setString(76, consumerUtil.removeAsciNull(lLfDisPrdtDt.getPLANCD()));
            pstmt.setDouble(77, Double.valueOf(consumerUtil.removeAsciNull(lLfDisPrdtDt.getPRDTPREMAMT())));
            pstmt.setDouble(78, Double.valueOf(consumerUtil.removeAsciNull(lLfDisPrdtDt.getINSDAMT())));
            pstmt.setDouble(79, Double.valueOf(consumerUtil.removeAsciNull(lLfDisPrdtDt.getAPRVAMT())));
            pstmt.setString(80, consumerUtil.removeAsciNull(lLfDisPrdtDt.getENTRANTSTSCD()));
            pstmt.setString(81, consumerUtil.removeAsciNull(lLfDisPrdtDt.getEOISTSCD()));
            pstmt.setDate(82, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lLfDisPrdtDt.getEOISTSDT().toString())));
            pstmt.setString(83, consumerUtil.removeAsciNull(lLfDisPrdtDt.getWOPCD()));
            pstmt.setDate(84, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lLfDisPrdtDt.getWOPEFFDT().toString())));
            pstmt.setDate(85, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lLfDisPrdtDt.getWOPCANCDT().toString())));
            pstmt.setString(86, consumerUtil.removeAsciNull(lLfDisPrdtDt.getACCLDTHCD()));
            pstmt.setDouble(87, Double.valueOf(consumerUtil.removeAsciNull(lLfDisPrdtDt.getACCLDTHPAYOAMT())));
            pstmt.setDate(88, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lLfDisPrdtDt.getACCLDTHEFFDT().toString())));
            pstmt.setDouble(89, Double.valueOf(consumerUtil.removeAsciNull(lLfDisPrdtDt.getREQAMT())));
            pstmt.setString(90, consumerUtil.removeAsciNull(lLfDisPrdtDt.getCOVSTSCD()));
            pstmt.setDouble(91, Double.valueOf(consumerUtil.removeAsciNull(lLfDisPrdtDt.getSALBENMULTFCT())));
            pstmt.setDouble(92, Double.valueOf(consumerUtil.removeAsciNull(lLfDisPrdtDt.getAGERDUCAPPLPCT())));
            pstmt.setInt(93, lLfDisPrdtDt.getRDUCAPPLAGENBR());
            pstmt.setString(94, consumerUtil.removeAsciNull(lLfDisPrdtDt.getCANCELRSNTYPCD()));
            pstmt.setDate(95, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lLfDisPrdtDt.getCOVPDTHRUDT().toString())));
            pstmt.setString(96, consumerUtil.removeAsciNull(lLfDisPrdtDt.getCOVPDTHRURSNCD()));
            pstmt.setString(97, consumerUtil.removeAsciNull(lLfDisPrdtDt.getLISTBILLTYPCD()));
            pstmt.setString(98, consumerUtil.removeAsciNull(lLfDisPrdtDt.getBILLINGSUFXCD()));
            pstmt.setString(99, consumerUtil.removeAsciNull(lLfDisPrdtDt.getBILLINGSUBGRPNBR()));
            pstmt.setDate(100, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lLfDisPrdtDt.getEBILLDT().toString())));
            pstmt.setString(101, consumerUtil.removeAsciNull(lLfDisPrdtDt.getLGCYBENPLNID()));
            pstmt.setString(102, consumerUtil.removeAsciNull(lLfDisPrdtDt.getLGCYPLNVARCD()));
            pstmt.setString(103, consumerUtil.removeAsciNull(lLfDisPrdtDt.getLGCYRPTCD()));
            pstmt.setString(104, consumerUtil.removeAsciNull(lLfDisPrdtDt.getUPDTTYPCD()));
            pstmt.setString(105, consumerUtil.removeAsciNull(lLfDisPrdtDt.getRACFID()));
            pstmt.setString(106, consumerUtil.removeAsciNull(lLfDisPrdtDt.getCESGRPNBR()));
            pstmt.setString(107, consumerUtil.removeAsciNull(lLfDisPrdtDt.getFUNDTYPCD()));
            pstmt.setString(108, consumerUtil.removeAsciNull(lLfDisPrdtDt.getSTATEOFISSUECD()));
            pstmt.setString(109, consumerUtil.removeAsciNull(lLfDisPrdtDt.getGRNDFATHEREDPOLIND()));
            pstmt.setString(110, consumerUtil.removeAsciNull(lLfDisPrdtDt.getDERIVCOVIND()));
            pstmt.setString(111, consumerUtil.removeAsciNull(lLfDisPrdtDt.getCNSMLGLENTYNM()));
            pstmt.setString(112, consumerUtil.removeAsciNull(lLfDisPrdtDt.getINDVGRPTYPCD()));
            pstmt.setString(113, consumerUtil.removeAsciNull(lLfDisPrdtDt.getBILTYPCD()));
            pstmt.setString(114, consumerUtil.removeAsciNull(lLfDisPrdtDt.getRATECOVTYPCD()));
            pstmt.setDate(115, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lLfDisPrdtDt.getPOLRENDT().toString())));
            pstmt.setString(116, utilityConfig.getSrcSysId());
            pstmt.setTimestamp(117, consumerUtil.getsysTimeStamp());
                pstmt.setTimestamp(118,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(lLfDisPrdtDt.getROWTMSTMP())));



            }});

        return batch_count[0];
    }

    public int delete(com.optum.exts.cdb.model.key.L_LF_DIS_PRDT_DT key )throws DataAccessException, SQLException {
        //String SQL = "UPDATE systest.CNSM_DTL SET row_sts_cd = ?  WHERE partn_nbr=? and cnsm_id=? and src_cd=? and lgcy_src_id = ? ";

        //String sql1 = utilityConfig.getQueryLookup().get("lCovPrdtDtDel").replace("<SCHEMA>", utilityConfig.getSchema());

        int affectedrows = 0;
        String sql = utilityConfig.getQueryLookup().get("lIfDisPrdtDtDel").replace("<SCHEMA>", utilityConfig.getSchema());

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
            pstmt.setString(4, consumerUtil.removeAsciNull(key.getSRCCD()));
            pstmt.setInt(5, key.getCNSMID());

            pstmt.setString(6, consumerUtil.removeAsciNull(key.getLGCYPOLNBR()));
            pstmt.setString(7, consumerUtil.removeAsciNull(key.getLGCYSRCID()));
            pstmt.setString(8, consumerUtil.removeAsciNull(key.getCOVTYPCD()));
            pstmt.setString(9, consumerUtil.removeAsciNull(key.getLGCYPRDTTYPCD()));
            pstmt.setString(10, consumerUtil.removeAsciNull(key.getCOVEFFDT().toString()));


        }});

        return batch_count[0];
    }
}
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
public class LHtlSrvDtWriteTransformer {

    private static final Logger log = LoggerFactory.getLogger(LHtlSrvDtWriteTransformer.class);


    @Autowired
    UtilityConfig utilityConfig;
    @Autowired
    ConsumerUtil consumerUtil;
    @Autowired
    JdbcTemplate jdbcTemplate;




    public int insertData(com.optum.exts.cdb.model.key.L_HLT_SRV_DT key, com.optum.exts.cdb.model.L_HLT_SRV_DT lHtlSrvDt)throws DataAccessException, SQLException {

//log.info("L_HLT_SRV_DT processing key "+key.toString());


        if((lHtlSrvDt.getROWUSERID().toString().trim()).equalsIgnoreCase("LINK/UNLINK") && (lHtlSrvDt.getROWSTSCD().toString().trim()).equalsIgnoreCase("D") ) {

            log.info("Skipping Record due to Link/Unlink Delete  :::" + key);
            return 0;
        }


        String sql = utilityConfig.getQueryLookup().get("lHltSrvDt").replace("<SCHEMA>", utilityConfig.getSchema());
        ;



        int[] batch_count= jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {

            @Override
            public int getBatchSize() {
                return 1;
            }

            @Override
            public void setValues(PreparedStatement pstmt, int i) throws SQLException {

            pstmt.setInt(1, lHtlSrvDt.getPARTNNBR());
            pstmt.setInt(2, lHtlSrvDt.getCNSMID());
            pstmt.setString(3, consumerUtil.removeAsciNull(lHtlSrvDt.getSRCCD()));
            pstmt.setString(4, consumerUtil.removeAsciNull(lHtlSrvDt.getLGCYPOLNBR()));
            pstmt.setString(5, consumerUtil.removeAsciNull(lHtlSrvDt.getLGCYSRCID()));
            pstmt.setString(6, consumerUtil.removeAsciNull(lHtlSrvDt.getHLTSRVPRDTLNCD()));
            pstmt.setString(7, consumerUtil.removeAsciNull(lHtlSrvDt.getHLTSRVPRDTCD()));
            pstmt.setDate(8, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lHtlSrvDt.getHLTSRVEFFDT().toString())));
            pstmt.setDate(9, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lHtlSrvDt.getHLTSRVCANCDT().toString())));
            pstmt.setString(10, consumerUtil.removeAsciNull(lHtlSrvDt.getCOSDIVCD()));
            pstmt.setString(11, consumerUtil.removeAsciNull(lHtlSrvDt.getCOSGRPNBR()));
            pstmt.setString(12, consumerUtil.removeAsciNull(lHtlSrvDt.getCOVLVLTYPCD()));
            pstmt.setString(13, consumerUtil.removeAsciNull(lHtlSrvDt.getSHRARNGTYPCD()));
            pstmt.setString(14, consumerUtil.removeAsciNull(lHtlSrvDt.getSHRARNGOBLIGCD()));
            pstmt.setString(15, consumerUtil.removeAsciNull(lHtlSrvDt.getEESTSTYPCD()));
            pstmt.setString(16, consumerUtil.removeAsciNull(lHtlSrvDt.getELIGSYSTYPCD()));
            pstmt.setString(17, consumerUtil.removeAsciNull(lHtlSrvDt.getROWSTSCD()));
            pstmt.setTimestamp(18, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(lHtlSrvDt.getSRCTMSTMP().toString())));
            pstmt.setTimestamp(19, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(lHtlSrvDt.getROWTMSTMP().toString())));
            pstmt.setInt(20, lHtlSrvDt.getPRFLID());
            pstmt.setString(21, consumerUtil.removeAsciNull(lHtlSrvDt.getROWUSERID()));
            pstmt.setString(22, consumerUtil.removeAsciNull(lHtlSrvDt.getSECTYPCD()));
            pstmt.setInt(23, lHtlSrvDt.getSRCCDBXREFID());
            pstmt.setInt(24, lHtlSrvDt.getXREFIDPARTNNBR());
            pstmt.setString(25, consumerUtil.removeAsciNull(lHtlSrvDt.getCANCELRSNTYPCD()));
            pstmt.setDate(26, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lHtlSrvDt.getCOVPDTHRUDT().toString())));
            pstmt.setString(27, consumerUtil.removeAsciNull(lHtlSrvDt.getCOVPDTHRURSNCD()));
            pstmt.setString(28, consumerUtil.removeAsciNull(lHtlSrvDt.getLISTBILLTYPCD()));
            pstmt.setString(29, consumerUtil.removeAsciNull(lHtlSrvDt.getBILLINGSUFXCD()));
            pstmt.setString(30, consumerUtil.removeAsciNull(lHtlSrvDt.getBILLINGSUBGRPNBR()));
            pstmt.setDate(31, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lHtlSrvDt.getEBILLDT().toString())));
            pstmt.setDate(32, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lHtlSrvDt.getRETROELIGRECVDT().toString())));
            pstmt.setString(33, consumerUtil.removeAsciNull(lHtlSrvDt.getRETRODAYS()));
            pstmt.setString(34, consumerUtil.removeAsciNull(lHtlSrvDt.getRETROTYPCD()));
            pstmt.setString(35, consumerUtil.removeAsciNull(lHtlSrvDt.getRETROOVRDTYPCD()));
            pstmt.setDate(36, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lHtlSrvDt.getRETROORIGCOVEFFDT().toString())));
            pstmt.setDate(37, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lHtlSrvDt.getRETROORIGCOVCANCDT().toString())));
            pstmt.setString(38, consumerUtil.removeAsciNull(lHtlSrvDt.getLGCYPLNVARCD()));
            pstmt.setString(39, consumerUtil.removeAsciNull(lHtlSrvDt.getLGCYRPTCD()));
            pstmt.setString(40, consumerUtil.removeAsciNull(lHtlSrvDt.getLGCYBENPLNID()));
            pstmt.setString(41, consumerUtil.removeAsciNull(lHtlSrvDt.getLGCYPRDTID()));
            pstmt.setString(42, consumerUtil.removeAsciNull(lHtlSrvDt.getUPDTTYPCD()));
            pstmt.setString(43, consumerUtil.removeAsciNull(lHtlSrvDt.getRACFID()));
            pstmt.setString(44, consumerUtil.removeAsciNull(lHtlSrvDt.getCESGRPNBR()));
            pstmt.setString(45, consumerUtil.removeAsciNull(lHtlSrvDt.getFUNDTYPCD()));
            pstmt.setString(46, consumerUtil.removeAsciNull(lHtlSrvDt.getSTATEOFISSUECD()));
            pstmt.setString(47, consumerUtil.removeAsciNull(lHtlSrvDt.getCOBRAMO()));
            pstmt.setString(48, consumerUtil.removeAsciNull(lHtlSrvDt.getCOBRAQUALEVNTCD()));
            pstmt.setDate(49, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lHtlSrvDt.getCOBRAEFFDT().toString())));
            pstmt.setString(50, consumerUtil.removeAsciNull(lHtlSrvDt.getGRNDFATHEREDPOLIND()));
            pstmt.setString(51, consumerUtil.removeAsciNull(lHtlSrvDt.getDERIVCOVIND()));
            pstmt.setString(52, consumerUtil.removeAsciNull(lHtlSrvDt.getTOPSCOVLVLTYPCD()));
            pstmt.setString(53, consumerUtil.removeAsciNull(lHtlSrvDt.getCNSMLGLENTYNM()));
            pstmt.setString(54, consumerUtil.removeAsciNull(lHtlSrvDt.getINDVGRPTYPCD()));
            pstmt.setString(55, consumerUtil.removeAsciNull(lHtlSrvDt.getBILTYPCD()));
            pstmt.setString(56, utilityConfig.getSrcSysId());
            pstmt.setTimestamp(57, consumerUtil.getsysTimeStamp());
            pstmt.setTimestamp(58, consumerUtil.getsysTimeStamp());

            pstmt.setDate(59, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lHtlSrvDt.getHLTSRVCANCDT().toString())));
            pstmt.setString(60, consumerUtil.removeAsciNull(lHtlSrvDt.getCOSDIVCD()));
            pstmt.setString(61, consumerUtil.removeAsciNull(lHtlSrvDt.getCOSGRPNBR()));
            pstmt.setString(62, consumerUtil.removeAsciNull(lHtlSrvDt.getCOVLVLTYPCD()));
            pstmt.setString(63, consumerUtil.removeAsciNull(lHtlSrvDt.getSHRARNGTYPCD()));
            pstmt.setString(64, consumerUtil.removeAsciNull(lHtlSrvDt.getSHRARNGOBLIGCD()));
            pstmt.setString(65, consumerUtil.removeAsciNull(lHtlSrvDt.getEESTSTYPCD()));
            pstmt.setString(66, consumerUtil.removeAsciNull(lHtlSrvDt.getELIGSYSTYPCD()));
            pstmt.setString(67, consumerUtil.removeAsciNull(lHtlSrvDt.getROWSTSCD()));
            pstmt.setTimestamp(68, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(lHtlSrvDt.getSRCTMSTMP().toString())));
            pstmt.setTimestamp(69, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(lHtlSrvDt.getROWTMSTMP().toString())));
            pstmt.setInt(70, lHtlSrvDt.getPRFLID());
            pstmt.setString(71, consumerUtil.removeAsciNull(lHtlSrvDt.getROWUSERID()));
            pstmt.setString(72, consumerUtil.removeAsciNull(lHtlSrvDt.getSECTYPCD()));
            pstmt.setInt(73, lHtlSrvDt.getCNSMID());
            pstmt.setInt(74, lHtlSrvDt.getPARTNNBR());
            pstmt.setString(75, consumerUtil.removeAsciNull(lHtlSrvDt.getCANCELRSNTYPCD()));
            pstmt.setDate(76, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lHtlSrvDt.getCOVPDTHRUDT().toString())));
            pstmt.setString(77, consumerUtil.removeAsciNull(lHtlSrvDt.getCOVPDTHRURSNCD()));
            pstmt.setString(78, consumerUtil.removeAsciNull(lHtlSrvDt.getLISTBILLTYPCD()));
            pstmt.setString(79, consumerUtil.removeAsciNull(lHtlSrvDt.getBILLINGSUFXCD()));
            pstmt.setString(80, consumerUtil.removeAsciNull(lHtlSrvDt.getBILLINGSUBGRPNBR()));
            pstmt.setDate(81, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lHtlSrvDt.getEBILLDT().toString())));
            pstmt.setDate(82, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lHtlSrvDt.getRETROELIGRECVDT().toString())));
            pstmt.setString(83, consumerUtil.removeAsciNull(lHtlSrvDt.getRETRODAYS()));
            pstmt.setString(84, consumerUtil.removeAsciNull(lHtlSrvDt.getRETROTYPCD()));
            pstmt.setString(85, consumerUtil.removeAsciNull(lHtlSrvDt.getRETROOVRDTYPCD()));
            pstmt.setDate(86, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lHtlSrvDt.getRETROORIGCOVEFFDT().toString())));
            pstmt.setDate(87, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lHtlSrvDt.getRETROORIGCOVCANCDT().toString())));
            pstmt.setString(88, consumerUtil.removeAsciNull(lHtlSrvDt.getLGCYPLNVARCD()));
            pstmt.setString(89, consumerUtil.removeAsciNull(lHtlSrvDt.getLGCYRPTCD()));
            pstmt.setString(90, consumerUtil.removeAsciNull(lHtlSrvDt.getLGCYBENPLNID()));
            pstmt.setString(91, consumerUtil.removeAsciNull(lHtlSrvDt.getLGCYPRDTID()));
            pstmt.setString(92, consumerUtil.removeAsciNull(lHtlSrvDt.getUPDTTYPCD()));
            pstmt.setString(93, consumerUtil.removeAsciNull(lHtlSrvDt.getRACFID()));
            pstmt.setString(94, consumerUtil.removeAsciNull(lHtlSrvDt.getCESGRPNBR()));
            pstmt.setString(95, consumerUtil.removeAsciNull(lHtlSrvDt.getFUNDTYPCD()));
            pstmt.setString(96, consumerUtil.removeAsciNull(lHtlSrvDt.getSTATEOFISSUECD()));
            pstmt.setString(97, consumerUtil.removeAsciNull(lHtlSrvDt.getCOBRAMO()));
            pstmt.setString(98, consumerUtil.removeAsciNull(lHtlSrvDt.getCOBRAQUALEVNTCD()));
            pstmt.setDate(99, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lHtlSrvDt.getCOBRAEFFDT().toString())));
            pstmt.setString(100, consumerUtil.removeAsciNull(lHtlSrvDt.getGRNDFATHEREDPOLIND()));
            pstmt.setString(101, consumerUtil.removeAsciNull(lHtlSrvDt.getDERIVCOVIND()));
            pstmt.setString(102, consumerUtil.removeAsciNull(lHtlSrvDt.getTOPSCOVLVLTYPCD()));
            pstmt.setString(103, consumerUtil.removeAsciNull(lHtlSrvDt.getCNSMLGLENTYNM()));
            pstmt.setString(104, consumerUtil.removeAsciNull(lHtlSrvDt.getINDVGRPTYPCD()));
            pstmt.setString(105, consumerUtil.removeAsciNull(lHtlSrvDt.getBILTYPCD()));
            pstmt.setString(106, utilityConfig.getSrcSysId());
            pstmt.setTimestamp(107, consumerUtil.getsysTimeStamp());
                pstmt.setTimestamp(108,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(lHtlSrvDt.getROWTMSTMP())));

               //log.info(pstmt.toString() + "+++++++++++++++++++++++++++++++++++++++");

            }});
       // update(key);
        return batch_count[0];
    }

    public int delete(com.optum.exts.cdb.model.key.L_HLT_SRV_DT key ) throws DataAccessException, SQLException{
        //String SQL = "UPDATE systest.CNSM_DTL SET row_sts_cd = ?  WHERE partn_nbr=? and cnsm_id=? and src_cd=? and lgcy_src_id = ? ";

        //String sql1 = utilityConfig.getQueryLookup().get("lCovPrdtDtDel").replace("<SCHEMA>", utilityConfig.getSchema());

        int affectedrows = 0;
        String sql = utilityConfig.getQueryLookup().get("lHltSrvDtDel").replace("<SCHEMA>", utilityConfig.getSchema());

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
                pstmt.setString(6, consumerUtil.removeAsciNull(key.getLGCYPOLNBR()));
                pstmt.setString(7, consumerUtil.removeAsciNull(key.getLGCYSRCID()));
                pstmt.setString(8, consumerUtil.removeAsciNull(key.getHLTSRVPRDTLNCD()));
                pstmt.setString(9, consumerUtil.removeAsciNull(key.getHLTSRVPRDTCD()));
                pstmt.setString(10,consumerUtil.removeAsciNull( key.getHLTSRVEFFDT().toString()));

               // log.info(pstmt.toString() + "+++++++++++++++++++++++++++++++++++++++");
               // affectedrows = pstmt.executeUpdate();
            }});


        return batch_count[0];
    }
}
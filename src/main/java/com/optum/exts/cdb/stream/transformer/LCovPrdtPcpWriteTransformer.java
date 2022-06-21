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
public class LCovPrdtPcpWriteTransformer {

    private static final Logger log = LoggerFactory.getLogger(LCovPrdtPcpWriteTransformer.class);


    @Autowired
    UtilityConfig utilityConfig;
    @Autowired
    ConsumerUtil consumerUtil;
    @Autowired
    JdbcTemplate jdbcTemplate;




    public long insertData(com.optum.exts.cdb.model.key.L_COV_PRDT_PCP key, com.optum.exts.cdb.model.L_COV_PRDT_PCP lCovPrdtPcp)throws DataAccessException, SQLException {

//log.info("L_COV_PRDT_PCP processing key "+key.toString());
        if((lCovPrdtPcp.getROWUSERID().toString().trim()).equalsIgnoreCase("LINK/UNLINK") && (lCovPrdtPcp.getROWSTSCD().toString().trim()).equalsIgnoreCase("D") ) {

            log.info("Skipping Record due to Link/Unlink Delete  :::" + key);
            return 1;
        }

        String sql = utilityConfig.getQueryLookup().get("lCovPrdtPcp").replace("<SCHEMA>", utilityConfig.getSchema());
        ;
        long id = 0;

        int[] batch_count= jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {

            @Override
            public int getBatchSize() {
                return 1;
            }

            @Override
            public void setValues(PreparedStatement pstmt, int i) throws SQLException {


            pstmt.setInt(1, lCovPrdtPcp.getPARTNNBR());
            pstmt.setInt(2, lCovPrdtPcp.getCNSMID());
            pstmt.setString(3, consumerUtil.removeAsciNull(lCovPrdtPcp.getSRCCD()));
            pstmt.setString(4, consumerUtil.removeAsciNull(lCovPrdtPcp.getLGCYSRCID()));
            pstmt.setString(5, consumerUtil.removeAsciNull(lCovPrdtPcp.getCOVTYPCD()));
            pstmt.setInt(6, lCovPrdtPcp.getPROVID());
            pstmt.setDate(7, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lCovPrdtPcp.getPCPEFFDT().toString())));
            pstmt.setDate(8, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lCovPrdtPcp.getPCPCANCDT().toString())));
            pstmt.setString(9, consumerUtil.removeAsciNull(lCovPrdtPcp.getUPDTTYPCD()));
            pstmt.setInt(10, lCovPrdtPcp.getSRCCDBXREFID());
            pstmt.setInt(11, lCovPrdtPcp.getXREFIDPARTNNBR());
            pstmt.setString(12, consumerUtil.removeAsciNull(lCovPrdtPcp.getRACFID()));
            pstmt.setString(13, consumerUtil.removeAsciNull(lCovPrdtPcp.getROWUSERID()));
            pstmt.setString(14, consumerUtil.removeAsciNull(lCovPrdtPcp.getROWSTSCD()));
            pstmt.setTimestamp(15, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(lCovPrdtPcp.getSRCTMSTMP().toString())));
            pstmt.setTimestamp(16, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(lCovPrdtPcp.getROWTMSTMP().toString())));
            pstmt.setInt(17, lCovPrdtPcp.getPROVCONTRID());
            pstmt.setInt(18, lCovPrdtPcp.getADRSEQNBR());
            pstmt.setString(19, consumerUtil.removeAsciNull(lCovPrdtPcp.getLGCYPRDTCD()));
            pstmt.setString(20, consumerUtil.removeAsciNull(lCovPrdtPcp.getMKTTYPCD()));
            pstmt.setInt(21, lCovPrdtPcp.getPROVMKTNBR());
            pstmt.setString(22, consumerUtil.removeAsciNull(lCovPrdtPcp.getIPANBR()));
            pstmt.setString(23, consumerUtil.removeAsciNull(lCovPrdtPcp.getPROVSPCLCD()));
            pstmt.setString(24, consumerUtil.removeAsciNull(lCovPrdtPcp.getCOSDIVCD()));
            pstmt.setDouble(25, Double.valueOf(lCovPrdtPcp.getCOSNTWKCD()));
            pstmt.setDouble(26, Double.valueOf(lCovPrdtPcp.getCOSPNLNBR()));
            pstmt.setString(27, consumerUtil.removeAsciNull(lCovPrdtPcp.getPCPTYPCD()));
            pstmt.setDouble(28, Double.valueOf(lCovPrdtPcp.getCOSPROVSPCLCD()));
            pstmt.setDouble(29, Double.valueOf(lCovPrdtPcp.getTAXIDNBR()));
            pstmt.setString(30, consumerUtil.removeAsciNull(lCovPrdtPcp.getTAXIDSUFXCD()));
            pstmt.setString(31, consumerUtil.removeAsciNull(lCovPrdtPcp.getRNDMTYPCD()));
            pstmt.setString(32, consumerUtil.removeAsciNull(lCovPrdtPcp.getTAXIDPRFXCD()));
            pstmt.setString(33, consumerUtil.removeAsciNull(lCovPrdtPcp.getSRCPROVID()));
            pstmt.setString(34, consumerUtil.removeAsciNull(lCovPrdtPcp.getSRCPROVIDTYPCD()));
            pstmt.setString(35, consumerUtil.removeAsciNull(lCovPrdtPcp.getPROVACOID()));
            pstmt.setString(36, utilityConfig.getSrcSysId());
            pstmt.setTimestamp(37, consumerUtil.getsysTimeStamp());
            pstmt.setTimestamp(38, consumerUtil.getsysTimeStamp());


            pstmt.setDate(39, (Date) consumerUtil.dateParser(lCovPrdtPcp.getPCPCANCDT().toString()));
            pstmt.setString(40, consumerUtil.removeAsciNull(lCovPrdtPcp.getUPDTTYPCD()));
            pstmt.setInt(41, lCovPrdtPcp.getCNSMID());
            pstmt.setInt(42, lCovPrdtPcp.getPARTNNBR());
            pstmt.setString(43, consumerUtil.removeAsciNull(lCovPrdtPcp.getRACFID()));
            pstmt.setString(44, consumerUtil.removeAsciNull(lCovPrdtPcp.getROWUSERID()));
            pstmt.setString(45, consumerUtil.removeAsciNull(lCovPrdtPcp.getROWSTSCD()));
            pstmt.setTimestamp(46, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(lCovPrdtPcp.getSRCTMSTMP().toString())));
            pstmt.setTimestamp(47, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(lCovPrdtPcp.getROWTMSTMP().toString())));
            pstmt.setInt(48, lCovPrdtPcp.getPROVCONTRID());
            pstmt.setInt(49, lCovPrdtPcp.getADRSEQNBR());
            pstmt.setString(50, consumerUtil.removeAsciNull(lCovPrdtPcp.getLGCYPRDTCD()));
            pstmt.setString(51, consumerUtil.removeAsciNull(lCovPrdtPcp.getMKTTYPCD()));
            pstmt.setInt(52, lCovPrdtPcp.getPROVMKTNBR());
            pstmt.setString(53, consumerUtil.removeAsciNull(lCovPrdtPcp.getIPANBR()));
            pstmt.setString(54, consumerUtil.removeAsciNull(lCovPrdtPcp.getPROVSPCLCD()));
            pstmt.setString(55, consumerUtil.removeAsciNull(lCovPrdtPcp.getCOSDIVCD()));
            pstmt.setDouble(56, Double.valueOf(lCovPrdtPcp.getCOSNTWKCD()));
            pstmt.setDouble(57, Double.valueOf(lCovPrdtPcp.getCOSPNLNBR()));
            pstmt.setString(58, consumerUtil.removeAsciNull(lCovPrdtPcp.getPCPTYPCD()));
            pstmt.setDouble(59, Double.valueOf(lCovPrdtPcp.getCOSPROVSPCLCD()));
            pstmt.setDouble(60, Double.valueOf(lCovPrdtPcp.getTAXIDNBR()));
            pstmt.setString(61, consumerUtil.removeAsciNull(lCovPrdtPcp.getTAXIDSUFXCD()));
            pstmt.setString(62, consumerUtil.removeAsciNull(lCovPrdtPcp.getRNDMTYPCD()));
            pstmt.setString(63, consumerUtil.removeAsciNull(lCovPrdtPcp.getTAXIDPRFXCD()));
            pstmt.setString(64, consumerUtil.removeAsciNull(lCovPrdtPcp.getSRCPROVID()));
            pstmt.setString(65, consumerUtil.removeAsciNull(lCovPrdtPcp.getSRCPROVIDTYPCD()));
            pstmt.setString(66, consumerUtil.removeAsciNull(lCovPrdtPcp.getPROVACOID()));
            pstmt.setString(67, utilityConfig.getSrcSysId());
            pstmt.setTimestamp(68, consumerUtil.getsysTimeStamp());
                pstmt.setTimestamp(69,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(lCovPrdtPcp.getROWTMSTMP())));



            }});

        return batch_count[0];
    }

    public int delete(com.optum.exts.cdb.model.key.L_COV_PRDT_PCP  key ) throws DataAccessException, SQLException{
        //String SQL = "UPDATE systest.CNSM_DTL SET row_sts_cd = ?  WHERE partn_nbr=? and cnsm_id=? and src_cd=? and lgcy_src_id = ? ";

        String sql = utilityConfig.getQueryLookup().get("lCovPrdtPcpDel").replace("<SCHEMA>", utilityConfig.getSchema());

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

            pstmt.setString(7, consumerUtil.removeAsciNull(key.getCOVTYPCD()));
            pstmt.setInt(8, key.getPROVID());
            pstmt.setString(9, consumerUtil.removeAsciNull(key.getPCPEFFDT().toString()));




        }});

        return batch_count[0];
    }
}
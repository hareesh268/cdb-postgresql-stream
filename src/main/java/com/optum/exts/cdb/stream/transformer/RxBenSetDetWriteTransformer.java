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
public class RxBenSetDetWriteTransformer {
    private static final Logger log = LoggerFactory.getLogger(RxBenSetDetWriteTransformer.class);


    @Autowired
    UtilityConfig utilityConfig;
    @Autowired
    ConsumerUtil consumerUtil;
    @Autowired
    private JdbcTemplate jdbcTemplate;


    public int insertData(com.optum.exts.cdb.model.key.RX_BEN_SET_DET key, com.optum.exts.cdb.model.RX_BEN_SET_DET rxBenSetDet) throws DataAccessException, SQLException {

        String sql = utilityConfig.getQueryLookup().get("rxBenSetDet").replace("<SCHEMA>", utilityConfig.getSchema());
        ;

        if ((rxBenSetDet.getROWUSERID().trim()).equalsIgnoreCase("LINK/UNLINK") && (rxBenSetDet.getROWSTSCD().trim()).equalsIgnoreCase("D")) {

            log.info("Skipping Record due to Link/Unlink Delete  :::" + key);
            return 1;
        }


        int[] batch_count = jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {

            @Override
            public int getBatchSize() {
                return 1;
            }

            @Override
            public void setValues(PreparedStatement pstmt, int i) throws SQLException {

                pstmt.setString(1, consumerUtil.removeAsciNull(rxBenSetDet.getSRCCD()));
                pstmt.setString(2, consumerUtil.removeAsciNull(rxBenSetDet.getLGCYPOLNBR()));
                pstmt.setString(3, consumerUtil.removeAsciNull(rxBenSetDet.getCOVTYPCD()));
                pstmt.setString(4, consumerUtil.removeAsciNull(rxBenSetDet.getLGCYBENPLNID()));
                pstmt.setDate(5, consumerUtil.dateParser(consumerUtil.removeAsciNull(rxBenSetDet.getRXBENSETDETEFFDT())));
                pstmt.setDate(6, consumerUtil.dateParser(consumerUtil.removeAsciNull(rxBenSetDet.getRXBENSETDETCANCDT())));
                pstmt.setString(7, consumerUtil.removeAsciNull(rxBenSetDet.getLGCYPRDTCD()));
                pstmt.setString(8, consumerUtil.removeAsciNull(rxBenSetDet.getLGCYPRDTID()));
                pstmt.setString(9, consumerUtil.removeAsciNull(rxBenSetDet.getFUNDTYPCD()));
                pstmt.setString(10, consumerUtil.removeAsciNull(rxBenSetDet.getSTATEOFISSUECD()));
                pstmt.setString(11, consumerUtil.removeAsciNull(rxBenSetDet.getPROCSTATE()));
                pstmt.setString(12, consumerUtil.removeAsciNull(rxBenSetDet.getPRDTSRVCTYPCD()));
                pstmt.setString(13, consumerUtil.removeAsciNull(rxBenSetDet.getHAPLNDSCNTCD()));
                pstmt.setString(14, consumerUtil.removeAsciNull(rxBenSetDet.getDEPNMAXAGENBR()));
                pstmt.setString(15, consumerUtil.removeAsciNull(rxBenSetDet.getDEPNSTDNTAGENBR()));
                pstmt.setString(16, consumerUtil.removeAsciNull(rxBenSetDet.getRXCONTYRIND()));
                pstmt.setString(17, consumerUtil.removeAsciNull(rxBenSetDet.getRXROLLOVERIND()));
                pstmt.setString(18, consumerUtil.removeAsciNull(rxBenSetDet.getRXAGERULECD()));
                pstmt.setString(19, consumerUtil.removeAsciNull(rxBenSetDet.getRXREIMCD()));
                pstmt.setString(20, consumerUtil.removeAsciNull(rxBenSetDet.getRXDIVCD()));
                pstmt.setDouble(21, Double.valueOf(consumerUtil.removeAsciNull(rxBenSetDet.getRXCOPAYTIER1AMT().toString())));
                pstmt.setDouble(22, Double.valueOf(consumerUtil.removeAsciNull(rxBenSetDet.getRXCOPAYTIER2AMT().toString())));
                pstmt.setDouble(23, Double.valueOf(consumerUtil.removeAsciNull(rxBenSetDet.getRXCOPAYTIER3AMT().toString())));
                pstmt.setDouble(24, Double.valueOf(consumerUtil.removeAsciNull(rxBenSetDet.getRXCOPAYTIER4AMT().toString())));
                pstmt.setDouble(25, Double.valueOf(consumerUtil.removeAsciNull(rxBenSetDet.getRXCOINSTIER1PCT().toString())));
                pstmt.setDouble(26, Double.valueOf(consumerUtil.removeAsciNull(rxBenSetDet.getRXCOINSTIER2PCT().toString())));
                pstmt.setDouble(27, Double.valueOf(consumerUtil.removeAsciNull(rxBenSetDet.getRXCOINSTIER3PCT().toString())));
                pstmt.setDouble(28, Double.valueOf(consumerUtil.removeAsciNull(rxBenSetDet.getRXCOINSTIER4PCT().toString())));
                pstmt.setString(29, consumerUtil.removeAsciNull(rxBenSetDet.getRXCOPAYDESCTXT()));
                pstmt.setString(30, consumerUtil.removeAsciNull(rxBenSetDet.getPRRCARRTYPCD()));
                pstmt.setString(31, consumerUtil.removeAsciNull(rxBenSetDet.getRXPRNTCARDCD()));
                pstmt.setString(32, consumerUtil.removeAsciNull(rxBenSetDet.getUPDTTYPCD()));
                pstmt.setString(33, consumerUtil.removeAsciNull(rxBenSetDet.getRACFID()));
                pstmt.setString(34, consumerUtil.removeAsciNull(rxBenSetDet.getROWUSERID()));
                pstmt.setString(35, consumerUtil.removeAsciNull(rxBenSetDet.getROWSTSCD()));
                pstmt.setTimestamp(36, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(rxBenSetDet.getSRCTMSTMP())));
                pstmt.setTimestamp(37, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(rxBenSetDet.getROWTMSTMP())));
                pstmt.setString(38, consumerUtil.removeAsciNull(rxBenSetDet.getGLBSLTNSPRDTIND()));
                pstmt.setString(39, utilityConfig.getSrcSysId());  //src_sys_id
                pstmt.setTimestamp(40, consumerUtil.getsysTimeStamp());  //created_dttm
                pstmt.setTimestamp(41, consumerUtil.getsysTimeStamp()); //updt_dttm


                pstmt.setDate(42, consumerUtil.dateParser(consumerUtil.removeAsciNull(rxBenSetDet.getRXBENSETDETCANCDT())));
                pstmt.setString(43, consumerUtil.removeAsciNull(rxBenSetDet.getLGCYPRDTCD()));
                pstmt.setString(44, consumerUtil.removeAsciNull(rxBenSetDet.getLGCYPRDTID()));
                pstmt.setString(45, consumerUtil.removeAsciNull(rxBenSetDet.getFUNDTYPCD()));
                pstmt.setString(46, consumerUtil.removeAsciNull(rxBenSetDet.getSTATEOFISSUECD()));
                pstmt.setString(47, consumerUtil.removeAsciNull(rxBenSetDet.getPROCSTATE()));
                pstmt.setString(48, consumerUtil.removeAsciNull(rxBenSetDet.getPRDTSRVCTYPCD()));
                pstmt.setString(49, consumerUtil.removeAsciNull(rxBenSetDet.getHAPLNDSCNTCD()));
                pstmt.setString(50, consumerUtil.removeAsciNull(rxBenSetDet.getDEPNMAXAGENBR()));
                pstmt.setString(51, consumerUtil.removeAsciNull(rxBenSetDet.getDEPNSTDNTAGENBR()));
                pstmt.setString(52, consumerUtil.removeAsciNull(rxBenSetDet.getRXCONTYRIND()));
                pstmt.setString(53, consumerUtil.removeAsciNull(rxBenSetDet.getRXROLLOVERIND()));
                pstmt.setString(54, consumerUtil.removeAsciNull(rxBenSetDet.getRXAGERULECD()));
                pstmt.setString(55, consumerUtil.removeAsciNull(rxBenSetDet.getRXREIMCD()));
                pstmt.setString(56, consumerUtil.removeAsciNull(rxBenSetDet.getRXDIVCD()));
                pstmt.setDouble(57, Double.valueOf(consumerUtil.removeAsciNull(rxBenSetDet.getRXCOPAYTIER1AMT().toString())));
                pstmt.setDouble(58, Double.valueOf(consumerUtil.removeAsciNull(rxBenSetDet.getRXCOPAYTIER2AMT().toString())));
                pstmt.setDouble(59, Double.valueOf(consumerUtil.removeAsciNull(rxBenSetDet.getRXCOPAYTIER3AMT().toString())));
                pstmt.setDouble(60, Double.valueOf(consumerUtil.removeAsciNull(rxBenSetDet.getRXCOPAYTIER4AMT().toString())));
                pstmt.setDouble(61, Double.valueOf(consumerUtil.removeAsciNull(rxBenSetDet.getRXCOINSTIER1PCT().toString())));
                pstmt.setDouble(62, Double.valueOf(consumerUtil.removeAsciNull(rxBenSetDet.getRXCOINSTIER2PCT().toString())));
                pstmt.setDouble(63, Double.valueOf(consumerUtil.removeAsciNull(rxBenSetDet.getRXCOINSTIER3PCT().toString())));
                pstmt.setDouble(64, Double.valueOf(consumerUtil.removeAsciNull(rxBenSetDet.getRXCOINSTIER4PCT().toString())));
                pstmt.setString(65, consumerUtil.removeAsciNull(rxBenSetDet.getRXCOPAYDESCTXT()));
                pstmt.setString(66, consumerUtil.removeAsciNull(rxBenSetDet.getPRRCARRTYPCD()));
                pstmt.setString(67, consumerUtil.removeAsciNull(rxBenSetDet.getRXPRNTCARDCD()));
                pstmt.setString(68, consumerUtil.removeAsciNull(rxBenSetDet.getUPDTTYPCD()));
                pstmt.setString(69, consumerUtil.removeAsciNull(rxBenSetDet.getRACFID()));
                pstmt.setString(70, consumerUtil.removeAsciNull(rxBenSetDet.getROWUSERID()));
                pstmt.setString(71, consumerUtil.removeAsciNull(rxBenSetDet.getROWSTSCD()));
                pstmt.setTimestamp(72, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(rxBenSetDet.getSRCTMSTMP())));
                pstmt.setTimestamp(73, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(rxBenSetDet.getROWTMSTMP())));
                pstmt.setString(74, consumerUtil.removeAsciNull(rxBenSetDet.getGLBSLTNSPRDTIND()));
                pstmt.setString(75, utilityConfig.getSrcSysId());  //src_sys_id
                pstmt.setTimestamp(76, consumerUtil.getsysTimeStamp()); //updt_dttm
                pstmt.setTimestamp(77,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(rxBenSetDet.getROWTMSTMP())));



            }
        });


        return batch_count[0];
    }

    public int delete(com.optum.exts.cdb.model.key.RX_BEN_SET_DET key) throws DataAccessException, SQLException {

        String sql = utilityConfig.getQueryLookup().get("rxBenSetDetDel").replace("<SCHEMA>", utilityConfig.getSchema());

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
                pstmt.setDate(7, consumerUtil.dateParser(consumerUtil.removeAsciNull(key.getRXBENSETDETEFFDT())));
            }
        });


        return batch_count[0];
    }


}




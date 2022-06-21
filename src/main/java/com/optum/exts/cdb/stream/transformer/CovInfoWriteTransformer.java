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
public class CovInfoWriteTransformer {

    private static final Logger log = LoggerFactory.getLogger(CovInfoWriteTransformer.class);


    @Autowired
    UtilityConfig utilityConfig;
    @Autowired
    ConsumerUtil consumerUtil;
    @Autowired
    private JdbcTemplate jdbcTemplate;


    public int insertData(com.optum.exts.cdb.model.key.COV_INFO key, com.optum.exts.cdb.model.COV_INFO covInfo)
            throws DataAccessException, SQLException {

        String sql = utilityConfig.getQueryLookup().get("covInfo").replace("<SCHEMA>", utilityConfig.getSchema());

//        if ((covInfo.getROWUSERID().trim()).equalsIgnoreCase("LINK/UNLINK") && (covInfo.getROWSTSCD().trim()).equalsIgnoreCase("D")) {
//
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


                pstmt.setString(1, consumerUtil.removeAsciNull(covInfo.getSRCCD()));
                pstmt.setString(2, consumerUtil.removeAsciNull(covInfo.getLGCYPOLNBR()));
                pstmt.setString(3, consumerUtil.removeAsciNull(covInfo.getCOVTYPCD()));
                pstmt.setString(4, consumerUtil.removeAsciNull(covInfo.getLGCYPLNVARCD()));
                pstmt.setString(5, consumerUtil.removeAsciNull(covInfo.getLGCYRPTCD()));
                pstmt.setDate(6, consumerUtil.dateParser(consumerUtil.removeAsciNull(covInfo.getCOVOFREFFDT())));
                pstmt.setString(7, consumerUtil.removeAsciNull(covInfo.getLGCYBENPLNID()));
                pstmt.setString(8, consumerUtil.removeAsciNull(covInfo.getLGCYPRDTCD()));
                pstmt.setString(9, consumerUtil.removeAsciNull(covInfo.getLGCYPRDTID()));
                pstmt.setString(10, consumerUtil.removeAsciNull(covInfo.getDEPCOVALLOWEDIND()));
                pstmt.setString(11, consumerUtil.removeAsciNull(covInfo.getDEPDIFFCOVALLOWEDIND()));
                pstmt.setString(12, consumerUtil.removeAsciNull(covInfo.getMKTTYPCD()));
                pstmt.setDate(13, consumerUtil.dateParser(consumerUtil.removeAsciNull(covInfo.getCOVOFRCANCDT())));
                pstmt.setString(14, consumerUtil.removeAsciNull(covInfo.getCLMSYSTYPCD()));
                pstmt.setString(15, consumerUtil.removeAsciNull(covInfo.getELIGSYSTYPCD()));
                pstmt.setString(16, consumerUtil.removeAsciNull(covInfo.getLISTBILLTYPCD()));
                pstmt.setString(17, consumerUtil.removeAsciNull(covInfo.getBILLINGSUFXCD()));
                pstmt.setString(18, consumerUtil.removeAsciNull(covInfo.getBILLINGSUBGRPNBR()));
                pstmt.setString(19, consumerUtil.removeAsciNull(covInfo.getOPTUMTYPCD()));
                pstmt.setString(20, consumerUtil.removeAsciNull(covInfo.getCLMACCTCD()));
                pstmt.setString(21, consumerUtil.removeAsciNull(covInfo.getPRXSTCONDELIGIND()));
                pstmt.setString(22, consumerUtil.removeAsciNull(covInfo.getRXBENDESCCD()));
                pstmt.setString(23, consumerUtil.removeAsciNull(covInfo.getMEDCOPREFDRUGLISTCD()));
                pstmt.setString(24, consumerUtil.removeAsciNull(covInfo.getREQCOVIND()));
                pstmt.setString(25, consumerUtil.removeAsciNull(covInfo.getCOVWAITPRD()));
                pstmt.setString(26, consumerUtil.removeAsciNull(covInfo.getCAPIND()));
                pstmt.setString(27, consumerUtil.removeAsciNull(covInfo.getCOVCERTIND()));
                pstmt.setString(28, consumerUtil.removeAsciNull(covInfo.getCOSDIVCD()));
                pstmt.setString(29, consumerUtil.removeAsciNull(covInfo.getCOSGRPNBR()));
                pstmt.setString(30, consumerUtil.removeAsciNull(covInfo.getCOVDESCTXT()));
                pstmt.setString(31, consumerUtil.removeAsciNull(covInfo.getUPDTTYPCD()));
                pstmt.setString(32, consumerUtil.removeAsciNull(covInfo.getRACFID()));
                pstmt.setString(33, consumerUtil.removeAsciNull(covInfo.getROWUSERID()));
                pstmt.setString(34, consumerUtil.removeAsciNull(covInfo.getROWSTSCD()));
                pstmt.setTimestamp(35, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(covInfo.getSRCTMSTMP())));
                pstmt.setTimestamp(36, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(covInfo.getROWTMSTMP())));
                pstmt.setString(37, consumerUtil.removeAsciNull(covInfo.getCESGRPNBR()));
                pstmt.setString(38, consumerUtil.removeAsciNull(covInfo.getDERIVCOVIND()));
                pstmt.setString(39, consumerUtil.removeAsciNull(covInfo.getHIOSID()));
                pstmt.setString(40, consumerUtil.removeAsciNull(covInfo.getMAIND()));
                pstmt.setString(41, consumerUtil.removeAsciNull(covInfo.getLTDIND()));
                pstmt.setString(42, utilityConfig.getSrcSysId());  //src_sys_id
                pstmt.setTimestamp(43, consumerUtil.getsysTimeStamp());  //created_dttm
                pstmt.setTimestamp(44, consumerUtil.getsysTimeStamp()); //updt_dttm
                //  pstmt.setString(45, consumerUtil.removeAsciNull(covInfo.getPBMID()));
                // This attributed need to be consumed from compact topic.
//                pstmt.setString(45,"N"); // pbm_id


                pstmt.setString(45, consumerUtil.removeAsciNull(covInfo.getLGCYBENPLNID()));
                pstmt.setString(46, consumerUtil.removeAsciNull(covInfo.getLGCYPRDTCD()));
                pstmt.setString(47, consumerUtil.removeAsciNull(covInfo.getLGCYPRDTID()));
                pstmt.setString(48, consumerUtil.removeAsciNull(covInfo.getDEPCOVALLOWEDIND()));
                pstmt.setString(49, consumerUtil.removeAsciNull(covInfo.getDEPDIFFCOVALLOWEDIND()));
                pstmt.setString(50, consumerUtil.removeAsciNull(covInfo.getMKTTYPCD()));
               // pstmt.setDate(51, consumerUtil.dateParser(consumerUtil.removeAsciNull(covInfo.getCOVOFRCANCDT())));
                pstmt.setString(51, consumerUtil.removeAsciNull(covInfo.getCLMSYSTYPCD()));
                pstmt.setString(52, consumerUtil.removeAsciNull(covInfo.getELIGSYSTYPCD()));
                pstmt.setString(53, consumerUtil.removeAsciNull(covInfo.getLISTBILLTYPCD()));
                pstmt.setString(54, consumerUtil.removeAsciNull(covInfo.getBILLINGSUFXCD()));
                pstmt.setString(55, consumerUtil.removeAsciNull(covInfo.getBILLINGSUBGRPNBR()));
                pstmt.setString(56, consumerUtil.removeAsciNull(covInfo.getOPTUMTYPCD()));
                pstmt.setString(57, consumerUtil.removeAsciNull(covInfo.getCLMACCTCD()));
                pstmt.setString(58, consumerUtil.removeAsciNull(covInfo.getPRXSTCONDELIGIND()));
                pstmt.setString(59, consumerUtil.removeAsciNull(covInfo.getRXBENDESCCD()));
                pstmt.setString(60, consumerUtil.removeAsciNull(covInfo.getMEDCOPREFDRUGLISTCD()));
                pstmt.setString(61, consumerUtil.removeAsciNull(covInfo.getREQCOVIND()));
                pstmt.setString(62, consumerUtil.removeAsciNull(covInfo.getCOVWAITPRD()));
                pstmt.setString(63, consumerUtil.removeAsciNull(covInfo.getCAPIND()));
                pstmt.setString(64, consumerUtil.removeAsciNull(covInfo.getCOVCERTIND()));
                pstmt.setString(65, consumerUtil.removeAsciNull(covInfo.getCOSDIVCD()));
                pstmt.setString(66, consumerUtil.removeAsciNull(covInfo.getCOSGRPNBR()));
                pstmt.setString(67, consumerUtil.removeAsciNull(covInfo.getCOVDESCTXT()));
                pstmt.setString(68, consumerUtil.removeAsciNull(covInfo.getUPDTTYPCD()));
                pstmt.setString(69, consumerUtil.removeAsciNull(covInfo.getRACFID()));
                pstmt.setString(70, consumerUtil.removeAsciNull(covInfo.getROWUSERID()));
                pstmt.setString(71, consumerUtil.removeAsciNull(covInfo.getROWSTSCD()));
                pstmt.setTimestamp(72, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(covInfo.getSRCTMSTMP())));
                pstmt.setTimestamp(73, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(covInfo.getROWTMSTMP())));
                pstmt.setString(74, consumerUtil.removeAsciNull(covInfo.getCESGRPNBR()));
                pstmt.setString(75, consumerUtil.removeAsciNull(covInfo.getDERIVCOVIND()));
                pstmt.setString(76, consumerUtil.removeAsciNull(covInfo.getHIOSID()));
                pstmt.setString(77, consumerUtil.removeAsciNull(covInfo.getMAIND()));
                pstmt.setString(78, consumerUtil.removeAsciNull(covInfo.getLTDIND()));
                pstmt.setString(79, utilityConfig.getSrcSysId());  //src_sys_id
                pstmt.setTimestamp(80, consumerUtil.getsysTimeStamp()); //updt_dttm
                pstmt.setTimestamp(81,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(covInfo.getROWTMSTMP())));

                //  pstmt.setString(83, consumerUtil.removeAsciNull(covInfo.getPBMID()));
//                pstmt.setString(83,"N"); // pbm_id

            }
        });


        return batch_count[0];
    }

    public int delete(com.optum.exts.cdb.model.key.COV_INFO key) throws DataAccessException, SQLException {

        String sql = utilityConfig.getQueryLookup().get("covInfoDel").replace("<SCHEMA>", utilityConfig.getSchema());

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
                pstmt.setString(6, consumerUtil.removeAsciNull(key.getLGCYPLNVARCD()));
                pstmt.setString(7,consumerUtil.removeAsciNull( key.getLGCYRPTCD()));
                pstmt.setDate(8, consumerUtil.dateParser(consumerUtil.removeAsciNull(key.getCOVOFREFFDT())));
                pstmt.setDate(9, consumerUtil.dateParser(consumerUtil.removeAsciNull(key.getCOVOFRCANCDT())));
            }
        });
        return batch_count[0];
    }
}
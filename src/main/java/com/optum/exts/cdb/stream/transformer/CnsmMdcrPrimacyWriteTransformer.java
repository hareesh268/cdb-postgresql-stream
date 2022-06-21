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

@Configuration
@ConfigurationProperties("spring.datasource")
public class CnsmMdcrPrimacyWriteTransformer {
    private static final Logger log = LoggerFactory.getLogger(CnsmMdcrPrimacyWriteTransformer.class);

    @Autowired
    UtilityConfig utilityConfig;
    @Autowired
    ConsumerUtil consumerUtil;
    @Autowired
    private JdbcTemplate jdbcTemplate;

    public int insertData(com.optum.exts.cdb.model.key.CNSM_MDCR_PRIMACY key, com.optum.exts.cdb.model.CNSM_MDCR_PRIMACY cnmsMdcrPrimacy)throws NumberFormatException,DataAccessException, SQLException {

        String sql = utilityConfig.getQueryLookup().get("cnmsMdcrPrimacy").replace("<SCHEMA>",utilityConfig.getSchema());

        if((cnmsMdcrPrimacy.getROWUSERID().trim()).equalsIgnoreCase("LINK/UNLINK") && (cnmsMdcrPrimacy.getROWSTSCD().trim()).equalsIgnoreCase("D") ) {
            log.info("Skipping Record due to Link/Unlink Delete  :::" + key);
            return 1;
        }

        int[] batch_count= jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {

            @Override
            public int getBatchSize() {
                return 1;
            }

            @Override
            public void setValues(PreparedStatement pstmt, int i) throws SQLException {
                pstmt.setInt(1,cnmsMdcrPrimacy.getPARTNNBR());
                pstmt.setInt(2,cnmsMdcrPrimacy.getCNSMID());
                pstmt.setString(3,consumerUtil.removeAsciNull(cnmsMdcrPrimacy.getSRCCD()));
                pstmt.setString(4,consumerUtil.removeAsciNull(cnmsMdcrPrimacy.getLGCYSRCID()));
                pstmt.setString(5,consumerUtil.removeAsciNull(cnmsMdcrPrimacy.getMDCRPARTTYPCD()));
                pstmt.setDate(6,consumerUtil.dateParser(consumerUtil.removeAsciNull(cnmsMdcrPrimacy.getMDCRPRIMACYEFFDT())));
                pstmt.setDate(7,consumerUtil.dateParser(consumerUtil.removeAsciNull(cnmsMdcrPrimacy.getMDCRPRIMACYCANCDT())));
                pstmt.setString(8,consumerUtil.removeAsciNull(cnmsMdcrPrimacy.getOIPRIMACYTYPCD()));
                pstmt.setString(9,consumerUtil.removeAsciNull(cnmsMdcrPrimacy.getUHGORDRBENTYPCD()));
                pstmt.setString(10,consumerUtil.removeAsciNull(cnmsMdcrPrimacy.getRXGRPNBR()));
                pstmt.setString(11,consumerUtil.removeAsciNull(cnmsMdcrPrimacy.getOICRDHLDRID()));
                pstmt.setString(12,consumerUtil.removeAsciNull(cnmsMdcrPrimacy.getRXPCNNBR()));
                pstmt.setString(13,consumerUtil.removeAsciNull(cnmsMdcrPrimacy.getRXBINNBR()));
                pstmt.setString(14,consumerUtil.removeAsciNull(cnmsMdcrPrimacy.getOINBR()));
                pstmt.setString(15,consumerUtil.removeAsciNull(cnmsMdcrPrimacy.getSRCUPDTTYPCD()));
                pstmt.setString(16,consumerUtil.removeAsciNull(cnmsMdcrPrimacy.getUPDTRSTRCTYPCD()));
                pstmt.setInt(17,cnmsMdcrPrimacy.getSRCCDBXREFID());
                pstmt.setInt(18,cnmsMdcrPrimacy.getXREFIDPARTNNBR());
                pstmt.setString(19,consumerUtil.removeAsciNull(cnmsMdcrPrimacy.getUPDTTYPCD()));
                pstmt.setString(20,consumerUtil.removeAsciNull(cnmsMdcrPrimacy.getRACFID()));
                pstmt.setString(21,consumerUtil.removeAsciNull(cnmsMdcrPrimacy.getROWUSERID()));
                pstmt.setString(22,consumerUtil.removeAsciNull(cnmsMdcrPrimacy.getROWSTSCD()));
                pstmt.setTimestamp(23,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnmsMdcrPrimacy.getSRCTMSTMP())));
                pstmt.setTimestamp(24,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnmsMdcrPrimacy.getROWTMSTMP())));
                pstmt.setString(25,consumerUtil.removeAsciNull(cnmsMdcrPrimacy.getRXCARRTELNBR()));
                pstmt.setString(26,consumerUtil.removeAsciNull(cnmsMdcrPrimacy.getMDCRXOVRIND()));
                pstmt.setString(27,utilityConfig.getSrcSysId());
                pstmt.setTimestamp(28,consumerUtil.getsysTimeStamp());
                pstmt.setTimestamp(29,consumerUtil.getsysTimeStamp());

                //Upsert query fieldssetting
                pstmt.setInt(30,cnmsMdcrPrimacy.getCNSMID());
                pstmt.setDate(31,consumerUtil.dateParser(consumerUtil.removeAsciNull(cnmsMdcrPrimacy.getMDCRPRIMACYCANCDT())));
                pstmt.setString(32,consumerUtil.removeAsciNull(cnmsMdcrPrimacy.getOIPRIMACYTYPCD()));
                pstmt.setString(33,consumerUtil.removeAsciNull(cnmsMdcrPrimacy.getUHGORDRBENTYPCD()));
                pstmt.setString(34,consumerUtil.removeAsciNull(cnmsMdcrPrimacy.getRXGRPNBR()));
                pstmt.setString(35,consumerUtil.removeAsciNull(cnmsMdcrPrimacy.getOICRDHLDRID()));
                pstmt.setString(36,consumerUtil.removeAsciNull(cnmsMdcrPrimacy.getRXPCNNBR()));
                pstmt.setString(37,consumerUtil.removeAsciNull(cnmsMdcrPrimacy.getRXBINNBR()));
                pstmt.setString(38,consumerUtil.removeAsciNull(cnmsMdcrPrimacy.getOINBR()));
                pstmt.setString(39,consumerUtil.removeAsciNull(cnmsMdcrPrimacy.getSRCUPDTTYPCD()));
                pstmt.setString(40,consumerUtil.removeAsciNull(cnmsMdcrPrimacy.getUPDTRSTRCTYPCD()));
                pstmt.setInt(41,cnmsMdcrPrimacy.getPARTNNBR());
                pstmt.setString(42,consumerUtil.removeAsciNull(cnmsMdcrPrimacy.getUPDTTYPCD()));
                pstmt.setString(43,consumerUtil.removeAsciNull(cnmsMdcrPrimacy.getRACFID()));
                pstmt.setString(44,consumerUtil.removeAsciNull(cnmsMdcrPrimacy.getROWUSERID()));
                pstmt.setString(45,consumerUtil.removeAsciNull(cnmsMdcrPrimacy.getROWSTSCD()));
                pstmt.setTimestamp(46,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnmsMdcrPrimacy.getSRCTMSTMP())));
                pstmt.setTimestamp(47,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnmsMdcrPrimacy.getROWTMSTMP())));
                pstmt.setString(48,consumerUtil.removeAsciNull(cnmsMdcrPrimacy.getRXCARRTELNBR()));
                pstmt.setString(49,consumerUtil.removeAsciNull(cnmsMdcrPrimacy.getMDCRXOVRIND()));
                pstmt.setString(50,utilityConfig.getSrcSysId());
                pstmt.setTimestamp(51,consumerUtil.getsysTimeStamp());
                pstmt.setTimestamp(52,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnmsMdcrPrimacy.getROWTMSTMP())));

            }});
        return batch_count[0];
    }

    public int delete(com.optum.exts.cdb.model.key.CNSM_MDCR_PRIMACY key) throws DataAccessException, SQLException{

        String sql = utilityConfig.getQueryLookup().get("cnmsMdcrPrimacyDel").replace("<SCHEMA>", utilityConfig.getSchema());

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
                pstmt.setString(6,consumerUtil.removeAsciNull( key.getLGCYSRCID()));
                pstmt.setString(7,consumerUtil.removeAsciNull( key.getMDCRPARTTYPCD()));
                pstmt.setDate(8, consumerUtil.dateParser(consumerUtil.removeAsciNull(key.getMDCRPRIMACYEFFDT())));
            }});


        return batch_count[0];
    }
}

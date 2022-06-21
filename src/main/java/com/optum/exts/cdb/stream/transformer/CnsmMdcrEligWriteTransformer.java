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
public class CnsmMdcrEligWriteTransformer {
    private static final Logger log = LoggerFactory.getLogger(CnsmMdcrEligWriteTransformer.class);

    @Autowired
    UtilityConfig utilityConfig;
    @Autowired
    ConsumerUtil consumerUtil;
    @Autowired
    private JdbcTemplate jdbcTemplate;

    public int insertData(com.optum.exts.cdb.model.key.CNSM_MDCR_ELIG key, com.optum.exts.cdb.model.CNSM_MDCR_ELIG cnmsMdcrElig)throws NumberFormatException,DataAccessException, SQLException {

        String sql = utilityConfig.getQueryLookup().get("cnsmMdcrElig").replace("<SCHEMA>",utilityConfig.getSchema());

        if((cnmsMdcrElig.getROWUSERID().trim()).equalsIgnoreCase("LINK/UNLINK") && (cnmsMdcrElig.getROWSTSCD().trim()).equalsIgnoreCase("D") ) {
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
                pstmt.setInt(1,cnmsMdcrElig.getPARTNNBR());
                pstmt.setInt(2,cnmsMdcrElig.getCNSMID());
                pstmt.setInt(3,cnmsMdcrElig.getXREFIDPARTNNBR());
                pstmt.setString(4,consumerUtil.removeAsciNull(cnmsMdcrElig.getSRCCD()));
                pstmt.setString(5,consumerUtil.removeAsciNull(cnmsMdcrElig.getLGCYSRCID()));
                pstmt.setString(6,consumerUtil.removeAsciNull(cnmsMdcrElig.getMDCRPARTTYPCD()));
                pstmt.setDate(7,consumerUtil.dateParser(consumerUtil.removeAsciNull(cnmsMdcrElig.getMDCRELIGPARTEFFDT())));
                pstmt.setDate(8,consumerUtil.dateParser(consumerUtil.removeAsciNull(cnmsMdcrElig.getMDCRELIGPARTCANCDT())));
                pstmt.setString(9,consumerUtil.removeAsciNull(cnmsMdcrElig.getMDCRELIGIND()));
                pstmt.setString(10,consumerUtil.removeAsciNull(cnmsMdcrElig.getSRCUPDTTYPCD()));
                pstmt.setString(11,consumerUtil.removeAsciNull(cnmsMdcrElig.getUPDTRSTRCTYPCD()));
                pstmt.setInt(12,cnmsMdcrElig.getSRCCDBXREFID());
                pstmt.setString(13,consumerUtil.removeAsciNull(cnmsMdcrElig.getUPDTTYPCD()));
                pstmt.setString(14,consumerUtil.removeAsciNull(cnmsMdcrElig.getRACFID()));
                pstmt.setString(15,consumerUtil.removeAsciNull(cnmsMdcrElig.getROWUSERID()));
                pstmt.setString(16,consumerUtil.removeAsciNull(cnmsMdcrElig.getROWSTSCD()));
                pstmt.setTimestamp(17,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnmsMdcrElig.getSRCTMSTMP())));
                pstmt.setTimestamp(18,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnmsMdcrElig.getROWTMSTMP())));
                pstmt.setString(19,utilityConfig.getSrcSysId());
                pstmt.setTimestamp(20,consumerUtil.getsysTimeStamp());
                pstmt.setTimestamp(21,consumerUtil.getsysTimeStamp());

                //Update query fields setup
                pstmt.setInt(22,cnmsMdcrElig.getCNSMID());
                pstmt.setDate(23,consumerUtil.dateParser(consumerUtil.removeAsciNull(cnmsMdcrElig.getMDCRELIGPARTCANCDT())));
                pstmt.setString(24,consumerUtil.removeAsciNull(cnmsMdcrElig.getMDCRELIGIND()));
                pstmt.setString(25,consumerUtil.removeAsciNull(cnmsMdcrElig.getSRCUPDTTYPCD()));
                pstmt.setString(26,consumerUtil.removeAsciNull(cnmsMdcrElig.getUPDTRSTRCTYPCD()));
                pstmt.setInt(27,cnmsMdcrElig.getPARTNNBR());
                pstmt.setString(28,consumerUtil.removeAsciNull(cnmsMdcrElig.getUPDTTYPCD()));
                pstmt.setString(29,consumerUtil.removeAsciNull(cnmsMdcrElig.getRACFID()));
                pstmt.setString(30,consumerUtil.removeAsciNull(cnmsMdcrElig.getROWUSERID()));
                pstmt.setString(31,consumerUtil.removeAsciNull(cnmsMdcrElig.getROWSTSCD()));
                pstmt.setTimestamp(32,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnmsMdcrElig.getSRCTMSTMP())));
                pstmt.setTimestamp(33,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnmsMdcrElig.getROWTMSTMP())));
                pstmt.setString(34,consumerUtil.removeAsciNull(utilityConfig.getSrcSysId()));
                pstmt.setTimestamp(35,consumerUtil.getsysTimeStamp());
                pstmt.setTimestamp(36,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnmsMdcrElig.getROWTMSTMP())));

            }});
        return batch_count[0];
    }

    public int delete(com.optum.exts.cdb.model.key.CNSM_MDCR_ELIG key) throws DataAccessException, SQLException{

        String sql = utilityConfig.getQueryLookup().get("cnsmMdcrEligDel").replace("<SCHEMA>", utilityConfig.getSchema());

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
                pstmt.setString(7, consumerUtil.removeAsciNull(key.getMDCRPARTTYPCD()));
                pstmt.setDate(8, consumerUtil.dateParser(consumerUtil.removeAsciNull(key.getMDCRELIGPARTEFFDT())));
            }});


        return batch_count[0];
    }
}

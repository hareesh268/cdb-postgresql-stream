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
public class CnsmPrxstCondWriteTransformer {
    private static final Logger log = LoggerFactory.getLogger(CnsmPrxstCondWriteTransformer.class);

    @Autowired
    UtilityConfig utilityConfig;
    @Autowired
    ConsumerUtil consumerUtil;
    @Autowired
    private JdbcTemplate jdbcTemplate;

    public int insertData(com.optum.exts.cdb.model.key.CNSM_PRXST_COND key, com.optum.exts.cdb.model.CNSM_PRXST_COND cnmsPrxstCond)throws NumberFormatException,DataAccessException, SQLException {

        String sql = utilityConfig.getQueryLookup().get("cnmsPrxstCond").replace("<SCHEMA>",utilityConfig.getSchema());

        if((cnmsPrxstCond.getROWUSERID().trim()).equalsIgnoreCase("LINK/UNLINK") && (cnmsPrxstCond.getROWSTSCD().trim()).equalsIgnoreCase("D") ) {
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
                pstmt.setInt(1,cnmsPrxstCond.getPARTNNBR());
                pstmt.setInt(2,cnmsPrxstCond.getCNSMID());
                pstmt.setString(3,consumerUtil.removeAsciNull(cnmsPrxstCond.getSRCCD()));
                pstmt.setString(4,consumerUtil.removeAsciNull(cnmsPrxstCond.getLGCYSRCID()));
                pstmt.setDate(5,consumerUtil.dateParser(consumerUtil.removeAsciNull(cnmsPrxstCond.getPRXSTDIAGEFFDT())));
                pstmt.setString(6,consumerUtil.removeAsciNull(cnmsPrxstCond.getPRXSTDIAGSTRTRNGCD()));
                pstmt.setString(7,consumerUtil.removeAsciNull(cnmsPrxstCond.getPRXSTDIAGSTOPRNGCD()));
                pstmt.setDate(8,consumerUtil.dateParser( consumerUtil.removeAsciNull(cnmsPrxstCond.getPRXSTDIAGCANCDT())));
                pstmt.setString(9,consumerUtil.removeAsciNull( consumerUtil.removeAsciNull(cnmsPrxstCond.getCLMDIAGPSTNTYPCD())));
                pstmt.setInt(10,cnmsPrxstCond.getSRCCDBXREFID());
                pstmt.setInt(11,cnmsPrxstCond.getXREFIDPARTNNBR());
                pstmt.setString(12,consumerUtil.removeAsciNull(cnmsPrxstCond.getUPDTTYPCD()));
                pstmt.setString(13,consumerUtil.removeAsciNull(cnmsPrxstCond.getRACFID()));
                pstmt.setString(14,consumerUtil.removeAsciNull(cnmsPrxstCond.getROWUSERID()));
                pstmt.setString(15,consumerUtil.removeAsciNull(cnmsPrxstCond.getROWSTSCD()));
                pstmt.setTimestamp(16,consumerUtil.timeStampParser( consumerUtil.removeAsciNull(cnmsPrxstCond.getSRCTMSTMP())));
                pstmt.setTimestamp(17,consumerUtil.timeStampParser( consumerUtil.removeAsciNull(cnmsPrxstCond.getROWTMSTMP())));
                pstmt.setString(18,consumerUtil.removeAsciNull(cnmsPrxstCond.getICDUSETYPCD()));
                pstmt.setString(19,utilityConfig.getSrcSysId());
                pstmt.setTimestamp(20,consumerUtil.getsysTimeStamp());
                pstmt.setTimestamp(21,consumerUtil.getsysTimeStamp());

                //Upsert query fields setting
                pstmt.setInt(22,cnmsPrxstCond.getPARTNNBR());
                pstmt.setString(23,consumerUtil.removeAsciNull(cnmsPrxstCond.getPRXSTDIAGSTOPRNGCD()));
                pstmt.setDate(24,consumerUtil.dateParser( consumerUtil.removeAsciNull(cnmsPrxstCond.getPRXSTDIAGCANCDT())));
                pstmt.setString(25,consumerUtil.removeAsciNull(cnmsPrxstCond.getCLMDIAGPSTNTYPCD()));
                pstmt.setInt(26,cnmsPrxstCond.getCNSMID());
                pstmt.setString(27,consumerUtil.removeAsciNull(cnmsPrxstCond.getUPDTTYPCD()));
                pstmt.setString(28,consumerUtil.removeAsciNull(cnmsPrxstCond.getRACFID()));
                pstmt.setString(29,consumerUtil.removeAsciNull(cnmsPrxstCond.getROWUSERID()));
                pstmt.setString(30,consumerUtil.removeAsciNull(cnmsPrxstCond.getROWSTSCD()));
                pstmt.setTimestamp(31,consumerUtil.timeStampParser( consumerUtil.removeAsciNull(cnmsPrxstCond.getSRCTMSTMP())));
                pstmt.setTimestamp(32,consumerUtil.timeStampParser( consumerUtil.removeAsciNull(cnmsPrxstCond.getROWTMSTMP())));
                pstmt.setString(33,consumerUtil.removeAsciNull(cnmsPrxstCond.getICDUSETYPCD()));
                pstmt.setString(34,utilityConfig.getSrcSysId());
                pstmt.setTimestamp(35,consumerUtil.getsysTimeStamp());
                pstmt.setTimestamp(36,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnmsPrxstCond.getROWTMSTMP())));

            }});
        return batch_count[0];
    }

    public int delete(com.optum.exts.cdb.model.key.CNSM_PRXST_COND key) throws DataAccessException, SQLException{

        String sql = utilityConfig.getQueryLookup().get("cnmsPrxstCondDel").replace("<SCHEMA>", utilityConfig.getSchema());

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
                pstmt.setDate(7, consumerUtil.dateParser( consumerUtil.removeAsciNull(key.getPRXSTDIAGEFFDT())));
                pstmt.setString(8, consumerUtil.removeAsciNull(key.getPRXSTDIAGSTRTRNGCD()));
            }});


        return batch_count[0];
    }
}

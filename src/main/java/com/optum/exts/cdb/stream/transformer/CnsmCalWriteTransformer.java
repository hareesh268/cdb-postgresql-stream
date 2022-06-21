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
public class CnsmCalWriteTransformer {
    private static final Logger log = LoggerFactory.getLogger(CnsmCalWriteTransformer.class);

    @Autowired
    UtilityConfig utilityConfig;
    @Autowired
    ConsumerUtil consumerUtil;
    @Autowired
    private JdbcTemplate jdbcTemplate;

    public int insertData(com.optum.exts.cdb.model.key.CNSM_CAL key, com.optum.exts.cdb.model.CNSM_CAL cnsmCal)throws NumberFormatException,DataAccessException, SQLException {

        String sql = utilityConfig.getQueryLookup().get("cnsmCal").replace("<SCHEMA>",utilityConfig.getSchema());

        if((cnsmCal.getROWUSERID().trim()).equalsIgnoreCase("LINK/UNLINK") && (cnsmCal.getROWSTSCD().trim()).equalsIgnoreCase("D") ) {
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
                pstmt.setInt(1,cnsmCal.getXREFIDPARTNNBR());
                pstmt.setInt(2,cnsmCal.getSRCCDBXREFID());
                pstmt.setString(3,consumerUtil.removeAsciNull(cnsmCal.getSRCCD()));
                pstmt.setString(4,consumerUtil.removeAsciNull(cnsmCal.getLGCYSRCID()));
                pstmt.setString(5,consumerUtil.removeAsciNull(cnsmCal.getCALCLMYRNBR()));
                pstmt.setString(6,consumerUtil.removeAsciNull(cnsmCal.getCALCLMMONBR()));
                pstmt.setString(7,consumerUtil.removeAsciNull(cnsmCal.getCALCLMDAYNBR()));
                pstmt.setString(8,consumerUtil.removeAsciNull(cnsmCal.getCALTYPCD()));
                pstmt.setDate(9,consumerUtil.dateParser(consumerUtil.removeAsciNull(cnsmCal.getCALRSNCDEFFDT())));
                pstmt.setString(10,consumerUtil.removeAsciNull(cnsmCal.getCALRSNTYPCD()));
                pstmt.setString(11,consumerUtil.removeAsciNull(cnsmCal.getUPDTTYPCD()));
                pstmt.setString(12,consumerUtil.removeAsciNull(cnsmCal.getRACFID()));
                pstmt.setString(13,consumerUtil.removeAsciNull(cnsmCal.getROWUSERID()));
                pstmt.setString(14,consumerUtil.removeAsciNull(cnsmCal.getROWSTSCD()));
                pstmt.setTimestamp(15,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmCal.getSRCTMSTMP())));
                pstmt.setTimestamp(16,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmCal.getROWTMSTMP())));
                pstmt.setInt(17,cnsmCal.getPARTNNBR());
                pstmt.setInt(18,cnsmCal.getCNSMID());
                pstmt.setString(19,utilityConfig.getSrcSysId());
                pstmt.setTimestamp(20,consumerUtil.getsysTimeStamp());
                pstmt.setTimestamp(21,consumerUtil.getsysTimeStamp());

                //Upsert query fields setting
                pstmt.setString(22,consumerUtil.removeAsciNull(cnsmCal.getCALCLMDAYNBR()));
                pstmt.setString(23,consumerUtil.removeAsciNull(cnsmCal.getCALTYPCD()));
                pstmt.setDate(24,consumerUtil.dateParser(consumerUtil.removeAsciNull(cnsmCal.getCALRSNCDEFFDT())));
                pstmt.setString(25,consumerUtil.removeAsciNull(cnsmCal.getCALRSNTYPCD()));
                pstmt.setString(26,consumerUtil.removeAsciNull(cnsmCal.getUPDTTYPCD()));
                pstmt.setString(27,consumerUtil.removeAsciNull(cnsmCal.getRACFID()));
                pstmt.setString(28,consumerUtil.removeAsciNull(cnsmCal.getROWUSERID()));
                pstmt.setString(29,consumerUtil.removeAsciNull(cnsmCal.getROWSTSCD()));
                pstmt.setTimestamp(30,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmCal.getSRCTMSTMP())));
                pstmt.setTimestamp(31,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmCal.getROWTMSTMP())));
                pstmt.setInt(32,cnsmCal.getPARTNNBR());
                pstmt.setInt(33,cnsmCal.getCNSMID());
                pstmt.setString(34,utilityConfig.getSrcSysId());
                pstmt.setTimestamp(35,consumerUtil.getsysTimeStamp());
                pstmt.setTimestamp(36,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmCal.getROWTMSTMP())));

            }});
        return batch_count[0];
    }

    public int delete(com.optum.exts.cdb.model.key.CNSM_CAL key) throws DataAccessException, SQLException{

        String sql = utilityConfig.getQueryLookup().get("cnsmCalDel").replace("<SCHEMA>", utilityConfig.getSchema());

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
                pstmt.setString(7, consumerUtil.removeAsciNull(key.getCALCLMYRNBR()));
                pstmt.setString(8, consumerUtil.removeAsciNull(key.getCALCLMMONBR()));
            }});
        return batch_count[0];
    }
}

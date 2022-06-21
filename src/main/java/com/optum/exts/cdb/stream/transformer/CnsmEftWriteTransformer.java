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
public class CnsmEftWriteTransformer {
    private static final Logger log = LoggerFactory.getLogger(CnsmEftWriteTransformer.class);

    @Autowired
    UtilityConfig utilityConfig;
    @Autowired
    ConsumerUtil consumerUtil;
    @Autowired
    private JdbcTemplate jdbcTemplate;

    public int insertData(com.optum.exts.cdb.model.key.CNSM_EFT key, com.optum.exts.cdb.model.CNSM_EFT cnsmEft)throws NumberFormatException,DataAccessException, SQLException {

        String sql = utilityConfig.getQueryLookup().get("cnsmEft").replace("<SCHEMA>",utilityConfig.getSchema());

        if((cnsmEft.getROWUSERID().trim()).equalsIgnoreCase("LINK/UNLINK") && (cnsmEft.getROWSTSCD().trim()).equalsIgnoreCase("D") ) {
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
                pstmt.setInt(1,cnsmEft.getPARTNNBR());
                pstmt.setInt(2,cnsmEft.getCNSMID());
                pstmt.setString(3,consumerUtil.removeAsciNull(cnsmEft.getSRCCD()));
                pstmt.setString(4,consumerUtil.removeAsciNull(cnsmEft.getLGCYSRCID()));
                pstmt.setString(5,consumerUtil.removeAsciNull(cnsmEft.getEFTTYPCD()));
                pstmt.setDate(6,consumerUtil.dateParser(consumerUtil.removeAsciNull((cnsmEft.getEFTEFFDT()))));
                pstmt.setDate(7,consumerUtil.dateParser(consumerUtil.removeAsciNull(cnsmEft.getEFTCANCDT())));
                pstmt.setInt(8,cnsmEft.getSRCCDBXREFID());
                pstmt.setInt(9,cnsmEft.getXREFIDPARTNNBR());
                pstmt.setString(10,consumerUtil.removeAsciNull(cnsmEft.getUPDTTYPCD()));
                pstmt.setString(11,consumerUtil.removeAsciNull(cnsmEft.getRACFID()));
                pstmt.setString(12,consumerUtil.removeAsciNull(cnsmEft.getROWUSERID()));
                pstmt.setString(13,consumerUtil.removeAsciNull(cnsmEft.getROWSTSCD()));
                pstmt.setTimestamp(14,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmEft.getSRCTMSTMP())));
                pstmt.setTimestamp(15, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmEft.getROWTMSTMP())));
                pstmt.setString(16,utilityConfig.getSrcSysId());
                pstmt.setTimestamp(17,consumerUtil.getsysTimeStamp());
                pstmt.setTimestamp(18,consumerUtil.getsysTimeStamp());

                //Update statement setting
                pstmt.setDate(19, consumerUtil.dateParser(cnsmEft.getEFTCANCDT()));
                pstmt.setInt(20,cnsmEft.getPARTNNBR());
                pstmt.setInt(21,cnsmEft.getCNSMID());
                pstmt.setString(22,consumerUtil.removeAsciNull(cnsmEft.getUPDTTYPCD()));
                pstmt.setString(23,consumerUtil.removeAsciNull(cnsmEft.getRACFID()));
                pstmt.setString(24,consumerUtil.removeAsciNull(cnsmEft.getROWUSERID()));
                pstmt.setString(25,consumerUtil.removeAsciNull(cnsmEft.getROWSTSCD()));
                pstmt.setTimestamp(26,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmEft.getSRCTMSTMP())));
                pstmt.setTimestamp(27, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmEft.getROWTMSTMP())));
                pstmt.setString(28,utilityConfig.getSrcSysId());
                pstmt.setTimestamp(29,consumerUtil.getsysTimeStamp());
                pstmt.setTimestamp(30,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmEft.getROWTMSTMP())));

            }});
        return batch_count[0];
    }

    public int delete(com.optum.exts.cdb.model.key.CNSM_EFT key) throws DataAccessException, SQLException{

        String sql = utilityConfig.getQueryLookup().get("cnsmEftDel").replace("<SCHEMA>", utilityConfig.getSchema());

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
                pstmt.setString(7, consumerUtil.removeAsciNull(key.getEFTTYPCD()));
                pstmt.setDate(8, consumerUtil.dateParser(consumerUtil.removeAsciNull(key.getEFTEFFDT())));
            }});
        return batch_count[0];
    }
}

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
public class CnsmSlryBasDedOopWriteTransformer {

    private static final Logger log = LoggerFactory.getLogger(CnsmSlryBasDedOopWriteTransformer.class);

    @Autowired
    UtilityConfig utilityConfig;
    @Autowired
    ConsumerUtil consumerUtil;
    @Autowired
    private JdbcTemplate jdbcTemplate;

    public int insertData(com.optum.exts.cdb.model.key.CNSM_SLRY_BAS_DED_OOP key, com.optum.exts.cdb.model.CNSM_SLRY_BAS_DED_OOP cnsmCustDefnFld)throws NumberFormatException,DataAccessException, SQLException {

        String sql = utilityConfig.getQueryLookup().get("cnsmSlryBasDedOop").replace("<SCHEMA>",utilityConfig.getSchema());

        if((cnsmCustDefnFld.getROWUSERID().trim()).equalsIgnoreCase("LINK/UNLINK") && (cnsmCustDefnFld.getROWSTSCD().trim()).equalsIgnoreCase("D") ) {
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
                pstmt.setInt(1,cnsmCustDefnFld.getPARTNNBR());
                pstmt.setInt(2,cnsmCustDefnFld.getCNSMID());
                pstmt.setString(3,consumerUtil.removeAsciNull(cnsmCustDefnFld.getSRCCD()));
                pstmt.setString(4,consumerUtil.removeAsciNull(cnsmCustDefnFld.getLGCYSRCID()));
                pstmt.setInt(5,cnsmCustDefnFld.getSLRYYR());
                pstmt.setString(6,consumerUtil.removeAsciNull(cnsmCustDefnFld.getCOVTYPCD()));
                pstmt.setDouble(7,Double.valueOf(consumerUtil.removeAsciNull(cnsmCustDefnFld.getSLRYINNDED().toString())));
                pstmt.setDouble(8,Double.valueOf(consumerUtil.removeAsciNull(cnsmCustDefnFld.getSLRYINNOOP().toString())));
                pstmt.setDouble(9,Double.valueOf(consumerUtil.removeAsciNull(cnsmCustDefnFld.getSLRYOONDED().toString())));
                pstmt.setDouble(10,Double.valueOf(consumerUtil.removeAsciNull(cnsmCustDefnFld.getSLRYOONOOP().toString())));
                pstmt.setInt(11,cnsmCustDefnFld.getSRCCDBXREFID());
                pstmt.setInt(12,cnsmCustDefnFld.getXREFIDPARTNNBR());
                pstmt.setString(13,consumerUtil.removeAsciNull(cnsmCustDefnFld.getUPDTTYPCD()));
                pstmt.setString(14,consumerUtil.removeAsciNull(cnsmCustDefnFld.getRACFID()));
                pstmt.setString(15,consumerUtil.removeAsciNull(cnsmCustDefnFld.getROWUSERID()));
                pstmt.setString(16,consumerUtil.removeAsciNull(cnsmCustDefnFld.getROWSTSCD()));
                pstmt.setTimestamp(17,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmCustDefnFld.getSRCTMSTMP())));
                pstmt.setTimestamp(18,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmCustDefnFld.getROWTMSTMP())));
                pstmt.setString(19,utilityConfig.getSrcSysId());
                pstmt.setTimestamp(20,consumerUtil.getsysTimeStamp());
                pstmt.setTimestamp(21,consumerUtil.getsysTimeStamp());

                //Upsert query fields setting
                pstmt.setInt(22,cnsmCustDefnFld.getPARTNNBR());
                pstmt.setDouble(23,Double.valueOf(consumerUtil.removeAsciNull(cnsmCustDefnFld.getSLRYINNDED().toString())));
                pstmt.setDouble(24,Double.valueOf(consumerUtil.removeAsciNull(cnsmCustDefnFld.getSLRYINNOOP().toString())));
                pstmt.setDouble(25,Double.valueOf(consumerUtil.removeAsciNull(cnsmCustDefnFld.getSLRYOONDED().toString())));
                pstmt.setDouble(26,Double.valueOf(consumerUtil.removeAsciNull(cnsmCustDefnFld.getSLRYOONOOP().toString())));
                pstmt.setInt(27,cnsmCustDefnFld.getCNSMID());
                pstmt.setString(28,consumerUtil.removeAsciNull(cnsmCustDefnFld.getUPDTTYPCD()));
                pstmt.setString(29,consumerUtil.removeAsciNull(cnsmCustDefnFld.getRACFID()));
                pstmt.setString(30,consumerUtil.removeAsciNull(cnsmCustDefnFld.getROWUSERID()));
                pstmt.setString(31,consumerUtil.removeAsciNull(cnsmCustDefnFld.getROWSTSCD()));
                pstmt.setTimestamp(32,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmCustDefnFld.getSRCTMSTMP())));
                pstmt.setTimestamp(33,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmCustDefnFld.getROWTMSTMP())));
                pstmt.setString(34,utilityConfig.getSrcSysId());
                pstmt.setTimestamp(35,consumerUtil.getsysTimeStamp());
                pstmt.setTimestamp(36,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmCustDefnFld.getROWTMSTMP())));

            }});
        return batch_count[0];
    }

    public int delete(com.optum.exts.cdb.model.key.CNSM_SLRY_BAS_DED_OOP key) throws DataAccessException, SQLException{
        String sql = utilityConfig.getQueryLookup().get("cnsmSlryBasDedOopDel").replace("<SCHEMA>", utilityConfig.getSchema());

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
                pstmt.setInt(7, key.getSLRYYR());
                pstmt.setString(8, consumerUtil.removeAsciNull(key.getCOVTYPCD()));
            }});


        return batch_count[0];
    }
}

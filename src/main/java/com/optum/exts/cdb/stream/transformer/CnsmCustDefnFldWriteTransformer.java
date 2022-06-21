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
public class CnsmCustDefnFldWriteTransformer {

    private static final Logger log = LoggerFactory.getLogger(CnsmCustDefnFldWriteTransformer.class);

    @Autowired
    UtilityConfig utilityConfig;
    @Autowired
    ConsumerUtil consumerUtil;
    @Autowired
    private JdbcTemplate jdbcTemplate;

    public int insertData(com.optum.exts.cdb.model.key.CNSM_CUST_DEFN_FLD key, com.optum.exts.cdb.model.CNSM_CUST_DEFN_FLD cnsmCustDefnFld)throws NumberFormatException,DataAccessException, SQLException {

        String sql = utilityConfig.getQueryLookup().get("cnsmCustDefnFld").replace("<SCHEMA>",utilityConfig.getSchema());

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
                pstmt.setInt(1,cnsmCustDefnFld.getXREFIDPARTNNBR());
                pstmt.setInt(2,cnsmCustDefnFld.getSRCCDBXREFID());
                pstmt.setString(3,consumerUtil.removeAsciNull(cnsmCustDefnFld.getSRCCD()));
                pstmt.setString(4,consumerUtil.removeAsciNull(cnsmCustDefnFld.getLGCYSRCID()));
                pstmt.setString(5,consumerUtil.removeAsciNull(cnsmCustDefnFld.getLGCYPOLNBR()));
                pstmt.setString(6,consumerUtil.removeAsciNull(cnsmCustDefnFld.getCUSTDEFNFLDTYPCD()));
                pstmt.setString(7,consumerUtil.removeAsciNull(cnsmCustDefnFld.getCUSTDEFNFLDTXT()));
                pstmt.setString(8,consumerUtil.removeAsciNull(cnsmCustDefnFld.getUPDTTYPCD()));
                pstmt.setString(9,consumerUtil.removeAsciNull(cnsmCustDefnFld.getRACFID()));
                pstmt.setString(10,consumerUtil.removeAsciNull(cnsmCustDefnFld.getROWUSERID()));
                pstmt.setString(11,consumerUtil.removeAsciNull(cnsmCustDefnFld.getROWSTSCD()));
                pstmt.setTimestamp(12,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmCustDefnFld.getSRCTMSTMP())));
                pstmt.setTimestamp(13,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmCustDefnFld.getROWTMSTMP())));
                pstmt.setString(14,consumerUtil.removeAsciNull(cnsmCustDefnFld.getMBRRPTCATNAME()));
                pstmt.setDate(15, consumerUtil.dateParser(consumerUtil.removeAsciNull(cnsmCustDefnFld.getMBRRPTEFFDT())));
                pstmt.setInt(16,cnsmCustDefnFld.getPARTNNBR());
                pstmt.setInt(17,cnsmCustDefnFld.getCNSMID());
                pstmt.setString(18,utilityConfig.getSrcSysId());
                pstmt.setTimestamp(19,consumerUtil.getsysTimeStamp());
                pstmt.setTimestamp(20,consumerUtil.getsysTimeStamp());

                //Upsert query fields setting
                pstmt.setString(21,consumerUtil.removeAsciNull(cnsmCustDefnFld.getCUSTDEFNFLDTXT()));
                pstmt.setString(22,consumerUtil.removeAsciNull(cnsmCustDefnFld.getUPDTTYPCD()));
                pstmt.setString(23,consumerUtil.removeAsciNull(cnsmCustDefnFld.getRACFID()));
                pstmt.setString(24,consumerUtil.removeAsciNull(cnsmCustDefnFld.getROWUSERID()));
                pstmt.setString(25,consumerUtil.removeAsciNull(cnsmCustDefnFld.getROWSTSCD()));
                pstmt.setTimestamp(26,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmCustDefnFld.getSRCTMSTMP())));
                pstmt.setTimestamp(27,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmCustDefnFld.getROWTMSTMP())));
                pstmt.setString(28,consumerUtil.removeAsciNull(cnsmCustDefnFld.getMBRRPTCATNAME()));
                pstmt.setDate(29,consumerUtil.dateParser(consumerUtil.removeAsciNull(cnsmCustDefnFld.getMBRRPTEFFDT())));
                pstmt.setInt(30,cnsmCustDefnFld.getPARTNNBR());
                pstmt.setInt(31,cnsmCustDefnFld.getCNSMID());
                pstmt.setString(32,utilityConfig.getSrcSysId());
                pstmt.setTimestamp(33,consumerUtil.getsysTimeStamp());
                pstmt.setTimestamp(34,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmCustDefnFld.getROWTMSTMP())));

            }});
        return batch_count[0];
    }

    public int delete(com.optum.exts.cdb.model.key.CNSM_CUST_DEFN_FLD key) throws DataAccessException, SQLException{

        String sql = utilityConfig.getQueryLookup().get("cnsmCustDefnFldDel").replace("<SCHEMA>", utilityConfig.getSchema());

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
                pstmt.setString(7, consumerUtil.removeAsciNull(key.getLGCYPOLNBR()));
                pstmt.setString(8, consumerUtil.removeAsciNull(key.getCUSTDEFNFLDTYPCD()));
            }});
        return batch_count[0];
    }
}

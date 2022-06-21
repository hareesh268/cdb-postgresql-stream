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
public class CnsmCovCustDefnFldWriteTransformer {

    private static final Logger log = LoggerFactory.getLogger(CnsmCovCustDefnFldWriteTransformer.class);

    @Autowired
    UtilityConfig utilityConfig;
    @Autowired
    ConsumerUtil consumerUtil;
    @Autowired
    private JdbcTemplate jdbcTemplate;

    public int insertData(com.optum.exts.cdb.model.key.CNSM_COV_CUST_DEFN_FLD key, com.optum.exts.cdb.model.CNSM_COV_CUST_DEFN_FLD cnsmCovCustDefnFld)throws NumberFormatException,DataAccessException, SQLException {

        String sql = utilityConfig.getQueryLookup().get("cnsmCovCustDefnFld").replace("<SCHEMA>",utilityConfig.getSchema());

        if((cnsmCovCustDefnFld.getROWUSERID().trim()).equalsIgnoreCase("LINK/UNLINK") && (cnsmCovCustDefnFld.getROWSTSCD().trim()).equalsIgnoreCase("D") ) {
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
                pstmt.setInt(1,cnsmCovCustDefnFld.getXREFIDPARTNNBR());
                pstmt.setInt(2,cnsmCovCustDefnFld.getSRCCDBXREFID());
                pstmt.setString(3,consumerUtil.removeAsciNull(cnsmCovCustDefnFld.getSRCCD()));
                pstmt.setString(4,consumerUtil.removeAsciNull(cnsmCovCustDefnFld.getLGCYPOLNBR()));
                pstmt.setString(5,consumerUtil.removeAsciNull(cnsmCovCustDefnFld.getLGCYSRCID()));
                pstmt.setString(6,consumerUtil.removeAsciNull(cnsmCovCustDefnFld.getCOVTYPCD()));
                pstmt.setDate(7,consumerUtil.dateParser(consumerUtil.removeAsciNull(cnsmCovCustDefnFld.getCOVEFFDT())));
                pstmt.setDate(8,consumerUtil.dateParser(consumerUtil.removeAsciNull(cnsmCovCustDefnFld.getCOVCANCDT())));
                pstmt.setString(9,consumerUtil.removeAsciNull(cnsmCovCustDefnFld.getCOVCUSTDEFNFLDCD()));
                pstmt.setString(10,consumerUtil.removeAsciNull(cnsmCovCustDefnFld.getCOVCUSTDEFNFLDTXT()));
                pstmt.setString(11,consumerUtil.removeAsciNull(cnsmCovCustDefnFld.getUPDTTYPCD()));
                pstmt.setString(12,consumerUtil.removeAsciNull(cnsmCovCustDefnFld.getRACFID()));
                pstmt.setString(13,consumerUtil.removeAsciNull(cnsmCovCustDefnFld.getROWUSERID()));
                pstmt.setString(14,consumerUtil.removeAsciNull(cnsmCovCustDefnFld.getROWSTSCD()));
                pstmt.setTimestamp(15,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmCovCustDefnFld.getSRCTMSTMP())));
                pstmt.setTimestamp(16,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmCovCustDefnFld.getROWTMSTMP())));
                pstmt.setInt(17,cnsmCovCustDefnFld.getPARTNNBR());
                pstmt.setInt(18,cnsmCovCustDefnFld.getCNSMID());
                pstmt.setString(19,consumerUtil.removeAsciNull(utilityConfig.getSrcSysId()));
                pstmt.setTimestamp(20,consumerUtil.getsysTimeStamp());
                pstmt.setTimestamp(21,consumerUtil.getsysTimeStamp());

                //Upsert query fields setting
                pstmt.setDate(22,consumerUtil.dateParser(cnsmCovCustDefnFld.getCOVCANCDT()));
                pstmt.setString(23,consumerUtil.removeAsciNull(cnsmCovCustDefnFld.getCOVCUSTDEFNFLDTXT()));
                pstmt.setString(24,consumerUtil.removeAsciNull(cnsmCovCustDefnFld.getUPDTTYPCD()));
                pstmt.setString(25,consumerUtil.removeAsciNull(cnsmCovCustDefnFld.getRACFID()));
                pstmt.setString(26,consumerUtil.removeAsciNull(cnsmCovCustDefnFld.getROWUSERID()));
                pstmt.setString(27,consumerUtil.removeAsciNull(cnsmCovCustDefnFld.getROWSTSCD()));
                pstmt.setTimestamp(28,consumerUtil.timeStampParser(cnsmCovCustDefnFld.getSRCTMSTMP()));
                pstmt.setTimestamp(29,consumerUtil.timeStampParser(cnsmCovCustDefnFld.getROWTMSTMP()));
                pstmt.setInt(30,cnsmCovCustDefnFld.getPARTNNBR());
                pstmt.setInt(31,cnsmCovCustDefnFld.getCNSMID());
                pstmt.setString(32,consumerUtil.removeAsciNull(utilityConfig.getSrcSysId()));
                pstmt.setTimestamp(33,consumerUtil.getsysTimeStamp());
                pstmt.setTimestamp(34,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmCovCustDefnFld.getROWTMSTMP())));

            }});
        return batch_count[0];
    }

    public int delete(com.optum.exts.cdb.model.key.CNSM_COV_CUST_DEFN_FLD key) throws DataAccessException, SQLException{

        String sql = utilityConfig.getQueryLookup().get("cnsmCovCustDefnFldDel").replace("<SCHEMA>", utilityConfig.getSchema());

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
                pstmt.setString(5, consumerUtil.removeAsciNull(key.getSRCCD().replace("\u0000", "")));
                pstmt.setString(6, consumerUtil.removeAsciNull(key.getLGCYSRCID().replace("\u0000", "")));
                pstmt.setString(7, consumerUtil.removeAsciNull(key.getLGCYPOLNBR().replace("\u0000", "")));
                pstmt.setString(8, consumerUtil.removeAsciNull(key.getCOVTYPCD()));
                pstmt.setDate(9, consumerUtil.dateParser(consumerUtil.removeAsciNull(key.getCOVEFFDT())));
                pstmt.setString(10, consumerUtil.removeAsciNull(key.getCOVCUSTDEFNFLDCD()));
            }});
        return batch_count[0];
    }
}

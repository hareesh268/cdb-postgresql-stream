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
public class CnsmAuthRepWriteTransformer {

    private static final Logger log = LoggerFactory.getLogger(CnsmAuthRepWriteTransformer.class);

    @Autowired
    UtilityConfig utilityConfig;
    @Autowired
    ConsumerUtil consumerUtil;
    @Autowired
    private JdbcTemplate jdbcTemplate;

    public int insertData(com.optum.exts.cdb.model.key.CNSM_AUTH_REP key, com.optum.exts.cdb.model.CNSM_AUTH_REP cnsmAuthRep)throws NumberFormatException,DataAccessException, SQLException {

        String sql = utilityConfig.getQueryLookup().get("cnsmAuthRep").replace("<SCHEMA>",utilityConfig.getSchema());

        if((cnsmAuthRep.getROWUSERID().trim()).equalsIgnoreCase("LINK/UNLINK") && (cnsmAuthRep.getROWSTSCD().trim()).equalsIgnoreCase("D") ) {
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
                pstmt.setInt(1,cnsmAuthRep.getXREFIDPARTNNBR());
                pstmt.setInt(2,cnsmAuthRep.getSRCCDBXREFID());
                pstmt.setString(3,consumerUtil.removeAsciNull(cnsmAuthRep.getSRCCD()));
                pstmt.setString(4,consumerUtil.removeAsciNull(cnsmAuthRep.getLGCYSRCID()));
                pstmt.setString(5,consumerUtil.removeAsciNull(cnsmAuthRep.getREPTYPCD()));
                pstmt.setString(6,consumerUtil.removeAsciNull(cnsmAuthRep.getREPLSTNM()));
                pstmt.setString(7,consumerUtil.removeAsciNull(cnsmAuthRep.getREPFSTNM()));
                pstmt.setString(8,consumerUtil.removeAsciNull(cnsmAuthRep.getREPMIDLNM()));
                pstmt.setString(9,consumerUtil.removeAsciNull(cnsmAuthRep.getREPNMSUFXTXT()));
                pstmt.setString(10,consumerUtil.removeAsciNull(cnsmAuthRep.getREPSLTN()));
                pstmt.setString(11,consumerUtil.removeAsciNull(cnsmAuthRep.getREPSTRADRLN1TXT()));
                pstmt.setString(12,consumerUtil.removeAsciNull(cnsmAuthRep.getREPSTRADRLN2TXT()));
                pstmt.setString(13,consumerUtil.removeAsciNull(cnsmAuthRep.getREPCTYNM()));
                pstmt.setString(14,consumerUtil.removeAsciNull(cnsmAuthRep.getREPPSTCD()));
                pstmt.setString(15,consumerUtil.removeAsciNull(cnsmAuthRep.getREPPSTEXTCD()));
                pstmt.setString(16,consumerUtil.removeAsciNull(cnsmAuthRep.getREPSTCD()));
                pstmt.setString(17,consumerUtil.removeAsciNull(cnsmAuthRep.getREPCNTRYCD()));
                pstmt.setString(18,consumerUtil.removeAsciNull(cnsmAuthRep.getUPDTTYPCD()));
                pstmt.setString(19,consumerUtil.removeAsciNull(cnsmAuthRep.getRACFID()));
                pstmt.setString(20,consumerUtil.removeAsciNull(cnsmAuthRep.getROWUSERID()));
                pstmt.setString(21,consumerUtil.removeAsciNull(cnsmAuthRep.getROWSTSCD()));
                pstmt.setTimestamp(22,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmAuthRep.getSRCTMSTMP())));
                pstmt.setTimestamp(23,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmAuthRep.getROWTMSTMP())));
                pstmt.setString(24,consumerUtil.removeAsciNull(cnsmAuthRep.getALTTELNBR()));
                pstmt.setString(25,consumerUtil.removeAsciNull(cnsmAuthRep.getCELLTELNBR()));
                pstmt.setString(26,consumerUtil.removeAsciNull(cnsmAuthRep.getPRIMTELNBR()));
                pstmt.setString(27,consumerUtil.removeAsciNull(cnsmAuthRep.getREPCNTRYSUBDIVCD()));
                pstmt.setString(28,consumerUtil.removeAsciNull(cnsmAuthRep.getELCTRADRTXT()));
                pstmt.setString(29,consumerUtil.removeAsciNull(cnsmAuthRep.getENTYIDTYPCD()));
                pstmt.setString(30,consumerUtil.removeAsciNull(cnsmAuthRep.getENTYID()));
                pstmt.setInt(31,cnsmAuthRep.getPARTNNBR());
                pstmt.setInt(32,cnsmAuthRep.getCNSMID());
                pstmt.setString(33,utilityConfig.getSrcSysId());
                pstmt.setTimestamp(34,consumerUtil.getsysTimeStamp());
                pstmt.setTimestamp(35,consumerUtil.getsysTimeStamp());

                //Upsert query fields setting
                pstmt.setString(36,consumerUtil.removeAsciNull(cnsmAuthRep.getREPLSTNM()));
                pstmt.setString(37,consumerUtil.removeAsciNull(cnsmAuthRep.getREPFSTNM()));
                pstmt.setString(38,consumerUtil.removeAsciNull(cnsmAuthRep.getREPMIDLNM()));
                pstmt.setString(39,consumerUtil.removeAsciNull(cnsmAuthRep.getREPNMSUFXTXT()));
                pstmt.setString(40,consumerUtil.removeAsciNull(cnsmAuthRep.getREPSLTN()));
                pstmt.setString(41,consumerUtil.removeAsciNull(cnsmAuthRep.getREPSTRADRLN1TXT()));
                pstmt.setString(42,consumerUtil.removeAsciNull(cnsmAuthRep.getREPSTRADRLN2TXT()));
                pstmt.setString(43,consumerUtil.removeAsciNull(cnsmAuthRep.getREPCTYNM()));
                pstmt.setString(44,consumerUtil.removeAsciNull(cnsmAuthRep.getREPPSTCD()));
                pstmt.setString(45,consumerUtil.removeAsciNull(cnsmAuthRep.getREPPSTEXTCD()));
                pstmt.setString(46,consumerUtil.removeAsciNull(cnsmAuthRep.getREPSTCD()));
                pstmt.setString(47,consumerUtil.removeAsciNull(cnsmAuthRep.getREPCNTRYCD()));
                pstmt.setString(48,consumerUtil.removeAsciNull(cnsmAuthRep.getUPDTTYPCD()));
                pstmt.setString(49,consumerUtil.removeAsciNull(cnsmAuthRep.getRACFID()));
                pstmt.setString(50,consumerUtil.removeAsciNull(cnsmAuthRep.getROWUSERID()));
                pstmt.setString(51,consumerUtil.removeAsciNull(cnsmAuthRep.getROWSTSCD()));
                pstmt.setTimestamp(52,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmAuthRep.getSRCTMSTMP())));
                pstmt.setTimestamp(53,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmAuthRep.getROWTMSTMP())));
                pstmt.setString(54,consumerUtil.removeAsciNull(cnsmAuthRep.getALTTELNBR()));
                pstmt.setString(55,consumerUtil.removeAsciNull(cnsmAuthRep.getCELLTELNBR()));
                pstmt.setString(56,consumerUtil.removeAsciNull(cnsmAuthRep.getPRIMTELNBR()));
                pstmt.setString(57,consumerUtil.removeAsciNull(cnsmAuthRep.getREPCNTRYSUBDIVCD()));
                pstmt.setString(58,consumerUtil.removeAsciNull(cnsmAuthRep.getELCTRADRTXT()));
                pstmt.setString(59,consumerUtil.removeAsciNull(cnsmAuthRep.getENTYIDTYPCD()));
                pstmt.setString(60,consumerUtil.removeAsciNull(cnsmAuthRep.getENTYID()));
                pstmt.setInt(61,cnsmAuthRep.getPARTNNBR());
                pstmt.setInt(62,cnsmAuthRep.getCNSMID());
                pstmt.setString(63,utilityConfig.getSrcSysId());
                pstmt.setTimestamp(64,consumerUtil.getsysTimeStamp());
                pstmt.setTimestamp(65,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmAuthRep.getROWTMSTMP())));

            }});
        return batch_count[0];
    }

    public int delete(com.optum.exts.cdb.model.key.CNSM_AUTH_REP key) throws DataAccessException, SQLException{

        String sql = utilityConfig.getQueryLookup().get("cnsmAuthRepDel").replace("<SCHEMA>", utilityConfig.getSchema());

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
                pstmt.setString(7,consumerUtil.removeAsciNull( key.getREPTYPCD()));
            }});
        return batch_count[0];
    }
}

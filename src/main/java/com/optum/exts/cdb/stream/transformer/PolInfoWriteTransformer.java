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
public class PolInfoWriteTransformer {
    private static final Logger log = LoggerFactory.getLogger(PolInfoWriteTransformer.class);


    @Autowired
    UtilityConfig utilityConfig;
    @Autowired
    ConsumerUtil consumerUtil;
    @Autowired
    private JdbcTemplate jdbcTemplate;


    public int insertData(com.optum.exts.cdb.model.key.POL_INFO key, com.optum.exts.cdb.model.POL_INFO polInfo) throws DataAccessException, SQLException {

        String sql = utilityConfig.getQueryLookup().get("polInfo").replace("<SCHEMA>", utilityConfig.getSchema());

        /*if ((polInfo.getROWUSERID().trim()).equalsIgnoreCase("LINK/UNLINK") && (polInfo.getROWSTSCD().trim()).equalsIgnoreCase("D")) {

            log.info("Skipping Record due to Link/Unlink Delete  :::" + key);
            return 1;
        }
*/

        int[] batch_count = jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {

            @Override
            public int getBatchSize() {
                return 1;
            }

            @Override
            public void setValues(PreparedStatement pstmt, int i) throws SQLException {

                pstmt.setString(1, consumerUtil.removeAsciNull(polInfo.getSRCCD()));
                pstmt.setString(2, consumerUtil.removeAsciNull(polInfo.getLGCYPOLNBR()));
                pstmt.setDate(3, consumerUtil.dateParser(consumerUtil.removeAsciNull(polInfo.getLGCYPOLEFFDT())));
                pstmt.setDate(4, consumerUtil.dateParser(consumerUtil.removeAsciNull(polInfo.getLGCYPOLCANCDT())));
                pstmt.setString(5, consumerUtil.removeAsciNull(polInfo.getLGCYSRCCUSTID()));
                pstmt.setString(6, consumerUtil.removeAsciNull(polInfo.getLGCYPOLNM()));
                pstmt.setString(7, consumerUtil.removeAsciNull(polInfo.getLGCYCLMENGCD()));
                pstmt.setString(8, consumerUtil.removeAsciNull(polInfo.getPOLSIZECD()));
                pstmt.setString(9, consumerUtil.removeAsciNull(polInfo.getPOLLEADPARTNERCD()));
                pstmt.setString(10, consumerUtil.removeAsciNull(polInfo.getUPDTTYPCD()));
                pstmt.setString(11, consumerUtil.removeAsciNull(polInfo.getRACFID()));
                pstmt.setString(12, consumerUtil.removeAsciNull(polInfo.getROWUSERID()));
                pstmt.setString(13, consumerUtil.removeAsciNull(polInfo.getROWSTSCD()));
                pstmt.setTimestamp(14, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(polInfo.getSRCTMSTMP())));
                pstmt.setTimestamp(15, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(polInfo.getROWTMSTMP())));
                pstmt.setString(16, utilityConfig.getSrcSysId());  //src_sys_id
                pstmt.setTimestamp(17, consumerUtil.getsysTimeStamp());  //created_dttm
                pstmt.setTimestamp(18, consumerUtil.getsysTimeStamp()); //updt_dttm


                pstmt.setDate(19, consumerUtil.dateParser(consumerUtil.removeAsciNull(polInfo.getLGCYPOLEFFDT())));
                pstmt.setDate(20, consumerUtil.dateParser(consumerUtil.removeAsciNull(polInfo.getLGCYPOLCANCDT())));
                pstmt.setString(21, consumerUtil.removeAsciNull(polInfo.getLGCYSRCCUSTID()));
                pstmt.setString(22, consumerUtil.removeAsciNull(polInfo.getLGCYPOLNM()));
                pstmt.setString(23, consumerUtil.removeAsciNull(polInfo.getLGCYCLMENGCD()));
                pstmt.setString(24, consumerUtil.removeAsciNull(polInfo.getPOLSIZECD()));
                pstmt.setString(25, consumerUtil.removeAsciNull(polInfo.getPOLLEADPARTNERCD()));
                pstmt.setString(26, consumerUtil.removeAsciNull(polInfo.getUPDTTYPCD()));
                pstmt.setString(27, consumerUtil.removeAsciNull(polInfo.getRACFID()));
                pstmt.setString(28, consumerUtil.removeAsciNull(polInfo.getROWUSERID()));
                pstmt.setString(29, consumerUtil.removeAsciNull(polInfo.getROWSTSCD()));
                pstmt.setTimestamp(30, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(polInfo.getSRCTMSTMP())));
                pstmt.setTimestamp(31, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(polInfo.getROWTMSTMP())));
                pstmt.setString(32, utilityConfig.getSrcSysId());  //src_sys_id
                pstmt.setTimestamp(33, consumerUtil.getsysTimeStamp()); //updt_dttm
                pstmt.setTimestamp(34,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(polInfo.getROWTMSTMP())));


            }
        });


        return batch_count[0];
    }

    public int delete(com.optum.exts.cdb.model.key.POL_INFO key) throws DataAccessException, SQLException {

        String sql = utilityConfig.getQueryLookup().get("polInfoDel").replace("<SCHEMA>", utilityConfig.getSchema());

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
            }
        });
        return batch_count[0];
    }
}

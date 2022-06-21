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
public class PlnBenSetWriteTransformer {

    private static final Logger log = LoggerFactory.getLogger(PlnBenSetWriteTransformer.class);


    @Autowired
    UtilityConfig utilityConfig;
    @Autowired
    ConsumerUtil consumerUtil;
    @Autowired
    private JdbcTemplate jdbcTemplate;


    public int insertData(com.optum.exts.cdb.model.key.PLN_BEN_SET key, com.optum.exts.cdb.model.PLN_BEN_SET plnBenSet) throws DataAccessException, SQLException {
        String sql = utilityConfig.getQueryLookup().get("plnBenSet").replace("<SCHEMA>", utilityConfig.getSchema());
        ;

        /*if ((plnBenSet.getROWUSERID().trim()).equalsIgnoreCase("LINK/UNLINK") && (plnBenSet.getROWSTSCD().trim()).equalsIgnoreCase("D")) {

            log.info("Skipping Record due to Link/Unlink Delete  :::" + key);
            return 1;
        }*/


        int[] batch_count = jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {

            @Override
            public int getBatchSize() {
                return 1;
            }

            @Override
            public void setValues(PreparedStatement pstmt, int i) throws SQLException {

                pstmt.setString(1, consumerUtil.removeAsciNull(plnBenSet.getSRCCD()));
                pstmt.setString(2, consumerUtil.removeAsciNull(plnBenSet.getLGCYPOLNBR()));
                pstmt.setString(3, consumerUtil.removeAsciNull(plnBenSet.getCOVTYPCD()));
                pstmt.setString(4, consumerUtil.removeAsciNull(plnBenSet.getLGCYBENPLNID()));
                pstmt.setDate(5, consumerUtil.dateParser(consumerUtil.removeAsciNull(plnBenSet.getLGCYBENPLNEFFDT())));
                pstmt.setDate(6, consumerUtil.dateParser(consumerUtil.removeAsciNull(plnBenSet.getLGCYBENPLNCANCDT())));
                pstmt.setString(7, consumerUtil.removeAsciNull(plnBenSet.getLGLENTY1NM()));
                pstmt.setString(8, consumerUtil.removeAsciNull(plnBenSet.getLGLENTY2NM()));
                pstmt.setString(9, consumerUtil.removeAsciNull(plnBenSet.getSHRARNGOBLIGCD()));
                pstmt.setString(10, consumerUtil.removeAsciNull(plnBenSet.getPHYSNSHRSVIND()));
                pstmt.setString(11, consumerUtil.removeAsciNull(plnBenSet.getOPTUMIND()));
                pstmt.setString(12, consumerUtil.removeAsciNull(plnBenSet.getPRODTERMCD()));
                pstmt.setString(13, consumerUtil.removeAsciNull(plnBenSet.getRETROTYPCD()));
                pstmt.setString(14, consumerUtil.removeAsciNull(plnBenSet.getRETRODAYS()));
                pstmt.setDate(15, consumerUtil.dateParser(consumerUtil.removeAsciNull(plnBenSet.getROLBENEFFDT())));
                pstmt.setString(16, consumerUtil.removeAsciNull(plnBenSet.getROLBENPERIODNBR()));
                pstmt.setString(17, consumerUtil.removeAsciNull(plnBenSet.getRXCOBTYPCD()));
                pstmt.setDate(18, consumerUtil.dateParser(consumerUtil.removeAsciNull(plnBenSet.getRXCOBEFFDT())));
                pstmt.setString(19, consumerUtil.removeAsciNull(plnBenSet.getOONIND()));
                pstmt.setString(20, consumerUtil.removeAsciNull(plnBenSet.getUPDTTYPCD()));
                pstmt.setString(21, consumerUtil.removeAsciNull(plnBenSet.getRACFID()));
                pstmt.setString(22, consumerUtil.removeAsciNull(plnBenSet.getROWUSERID()));
                pstmt.setString(23, consumerUtil.removeAsciNull(plnBenSet.getROWSTSCD()));
                pstmt.setTimestamp(24, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(plnBenSet.getSRCTMSTMP())));
                pstmt.setTimestamp(25, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(plnBenSet.getROWTMSTMP())));
                pstmt.setString(26, consumerUtil.removeAsciNull(plnBenSet.getERWRAPIND()));
                pstmt.setString(27, consumerUtil.removeAsciNull(plnBenSet.getSSPIND()));
                pstmt.setString(28, consumerUtil.removeAsciNull(plnBenSet.getOLRIND()));
                pstmt.setString(29, consumerUtil.removeAsciNull(plnBenSet.getMTLLVLCD()));
                pstmt.setString(30, utilityConfig.getSrcSysId());  //src_sys_id
                pstmt.setTimestamp(31, consumerUtil.getsysTimeStamp());  //created_dttm
                pstmt.setTimestamp(32, consumerUtil.getsysTimeStamp()); //updt_dttm


                pstmt.setDate(33, consumerUtil.dateParser(consumerUtil.removeAsciNull(plnBenSet.getLGCYBENPLNEFFDT())));
                pstmt.setDate(34, consumerUtil.dateParser(consumerUtil.removeAsciNull(plnBenSet.getLGCYBENPLNCANCDT())));
                pstmt.setString(35, consumerUtil.removeAsciNull(plnBenSet.getLGLENTY1NM()));
                pstmt.setString(36, consumerUtil.removeAsciNull(plnBenSet.getLGLENTY2NM()));
                pstmt.setString(37, consumerUtil.removeAsciNull(plnBenSet.getSHRARNGOBLIGCD()));
                pstmt.setString(38, consumerUtil.removeAsciNull(plnBenSet.getPHYSNSHRSVIND()));
                pstmt.setString(39, consumerUtil.removeAsciNull(plnBenSet.getOPTUMIND()));
                pstmt.setString(40, consumerUtil.removeAsciNull(plnBenSet.getPRODTERMCD()));
                pstmt.setString(41, consumerUtil.removeAsciNull(plnBenSet.getRETROTYPCD()));
                pstmt.setString(42, consumerUtil.removeAsciNull(plnBenSet.getRETRODAYS()));
                pstmt.setDate(43, consumerUtil.dateParser(consumerUtil.removeAsciNull(plnBenSet.getROLBENEFFDT())));
                pstmt.setString(44, consumerUtil.removeAsciNull(plnBenSet.getROLBENPERIODNBR()));
                pstmt.setString(45, consumerUtil.removeAsciNull(plnBenSet.getRXCOBTYPCD()));
                pstmt.setDate(46, consumerUtil.dateParser(plnBenSet.getRXCOBEFFDT()));
                pstmt.setString(47, consumerUtil.removeAsciNull(plnBenSet.getOONIND()));
                pstmt.setString(48, consumerUtil.removeAsciNull(plnBenSet.getUPDTTYPCD()));
                pstmt.setString(49, consumerUtil.removeAsciNull(plnBenSet.getRACFID()));
                pstmt.setString(50, consumerUtil.removeAsciNull(plnBenSet.getROWUSERID()));
                pstmt.setString(51, consumerUtil.removeAsciNull(plnBenSet.getROWSTSCD()));
                pstmt.setTimestamp(52, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(plnBenSet.getSRCTMSTMP())));
                pstmt.setTimestamp(53, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(plnBenSet.getROWTMSTMP())));
                pstmt.setString(54, consumerUtil.removeAsciNull(plnBenSet.getERWRAPIND()));
                pstmt.setString(55, consumerUtil.removeAsciNull(plnBenSet.getSSPIND()));
                pstmt.setString(56, consumerUtil.removeAsciNull(plnBenSet.getOLRIND()));
                pstmt.setString(57, consumerUtil.removeAsciNull(plnBenSet.getMTLLVLCD()));
                pstmt.setString(58, utilityConfig.getSrcSysId());  //src_sys_id
                pstmt.setTimestamp(59, consumerUtil.getsysTimeStamp()); //updt_dttm
                pstmt.setTimestamp(60,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(plnBenSet.getROWTMSTMP())));



            }
        });


        return batch_count[0];
    }

    public int delete(com.optum.exts.cdb.model.key.PLN_BEN_SET key) throws DataAccessException, SQLException {

        String sql = utilityConfig.getQueryLookup().get("plnBenSetDel").replace("<SCHEMA>", utilityConfig.getSchema());

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
                pstmt.setString(5,consumerUtil.removeAsciNull( key.getCOVTYPCD()));
                pstmt.setString(6, consumerUtil.removeAsciNull(key.getLGCYBENPLNID()));


            }
        });


        return batch_count[0];
    }


}


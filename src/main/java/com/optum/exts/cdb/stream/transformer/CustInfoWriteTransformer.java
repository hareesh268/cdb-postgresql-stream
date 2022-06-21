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
public class CustInfoWriteTransformer {

    private static final Logger log = LoggerFactory.getLogger(CustInfoWriteTransformer.class);


    @Autowired
    UtilityConfig utilityConfig;
    @Autowired
    ConsumerUtil consumerUtil;
    @Autowired
    private JdbcTemplate jdbcTemplate;


    public int insertData(com.optum.exts.cdb.model.key.CUST_INFO key, com.optum.exts.cdb.model.CUST_INFO custInfo) throws DataAccessException, SQLException {

        String sql = utilityConfig.getQueryLookup().get("custInfo").replace("<SCHEMA>", utilityConfig.getSchema());
        ;

       /* if ((custInfo.getROWUSERID().trim()).equalsIgnoreCase("LINK/UNLINK") && (custInfo.getROWSTSCD().trim()).equalsIgnoreCase("D")) {

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

                pstmt.setString(1, consumerUtil.removeAsciNull(custInfo.getSRCCD()));
                pstmt.setString(2, consumerUtil.removeAsciNull(custInfo.getLGCYSRCCUSTID()));
                pstmt.setString(3, consumerUtil.removeAsciNull(custInfo.getCUSTNM()));
                pstmt.setDate(4, consumerUtil.dateParser( consumerUtil.removeAsciNull(custInfo.getCUSTIDEFFDT())));
                pstmt.setDate(5, consumerUtil.dateParser( consumerUtil.removeAsciNull(custInfo.getCUSTIDCANCDT())));
                pstmt.setString(6, consumerUtil.removeAsciNull(custInfo.getELIGLOCTYPCD()));
                pstmt.setString(7, consumerUtil.removeAsciNull(custInfo.getMDCRXOVRCD()));
                pstmt.setString(8, consumerUtil.removeAsciNull(custInfo.getPSEUDOPCPTYPCD()));
                pstmt.setString(9, consumerUtil.removeAsciNull(custInfo.getASGNCOVLVLTYPIND()));
                pstmt.setString(10, consumerUtil.removeAsciNull(custInfo.getMKTSEGTYPCD()));
                pstmt.setString(11, consumerUtil.removeAsciNull(custInfo.getLABTYPCD()));
                pstmt.setString(12, consumerUtil.removeAsciNull(custInfo.getDIFFFAMMBRADRIND()));
                pstmt.setString(13, consumerUtil.removeAsciNull(custInfo.getCUSTCANCRSNTYPCD()));
                pstmt.setString(14, consumerUtil.removeAsciNull(custInfo.getALTIDASGNTYPCD()));
                pstmt.setString(15, consumerUtil.removeAsciNull(custInfo.getELIGANALYST1RACFID()));
                pstmt.setString(16, consumerUtil.removeAsciNull(custInfo.getELIGANALYST2RACFID()));
                pstmt.setString(17, consumerUtil.removeAsciNull(custInfo.getELIGANALYST1NM()));
                pstmt.setString(18, consumerUtil.removeAsciNull(custInfo.getELIGANALYST2NM()));
                pstmt.setString(19, consumerUtil.removeAsciNull(custInfo.getSTRCTSRCCD()));
                pstmt.setString(20, consumerUtil.removeAsciNull(custInfo.getRENMOCD()));
                pstmt.setString(21, consumerUtil.removeAsciNull(custInfo.getRENYRNBR()));
                pstmt.setString(22, consumerUtil.removeAsciNull(custInfo.getCUSTLGLNM()));
                pstmt.setDouble(23, Double.valueOf(custInfo.getCUSTTAXID()));
                pstmt.setInt(24, custInfo.getCUSTLOCNBR());
                pstmt.setString(25, consumerUtil.removeAsciNull(custInfo.getOFCOFSALECD()));
                pstmt.setString(26, consumerUtil.removeAsciNull(custInfo.getUPDTTYPCD()));
                pstmt.setString(27, consumerUtil.removeAsciNull(custInfo.getRACFID()));
                pstmt.setString(28, consumerUtil.removeAsciNull(custInfo.getROWUSERID()));
                pstmt.setString(29, consumerUtil.removeAsciNull(custInfo.getROWSTSCD()));
                pstmt.setTimestamp(30, consumerUtil.timeStampParser( consumerUtil.removeAsciNull(custInfo.getSRCTMSTMP())));
                pstmt.setTimestamp(31, consumerUtil.timeStampParser( consumerUtil.removeAsciNull(custInfo.getROWTMSTMP())));
                pstmt.setString(32, consumerUtil.removeAsciNull(custInfo.getRPTSEGTYPCD()));
                pstmt.setString(33, consumerUtil.removeAsciNull(custInfo.getINTGRCARDTYPCD()));
                pstmt.setString(34, consumerUtil.removeAsciNull(custInfo.getHISTPURGEDAYS()));
                pstmt.setDate(35, consumerUtil.dateParser( consumerUtil.removeAsciNull(custInfo.getOPTUMDT())));
                pstmt.setString(36, consumerUtil.removeAsciNull(custInfo.getORGTYPCD()));
                pstmt.setString(37, consumerUtil.removeAsciNull(custInfo.getMIGSRCCD()));
                pstmt.setString(38, utilityConfig.getSrcSysId());  //src_sys_id
                pstmt.setTimestamp(39, consumerUtil.getsysTimeStamp());  //created_dttm
                pstmt.setTimestamp(40, consumerUtil.getsysTimeStamp()); //updt_dttm


                pstmt.setString(41, consumerUtil.removeAsciNull(custInfo.getCUSTNM()));
                pstmt.setDate(42, consumerUtil.dateParser( consumerUtil.removeAsciNull(custInfo.getCUSTIDEFFDT())));
                pstmt.setDate(43, consumerUtil.dateParser( consumerUtil.removeAsciNull(custInfo.getCUSTIDCANCDT())));
                pstmt.setString(44, consumerUtil.removeAsciNull(custInfo.getELIGLOCTYPCD()));
                pstmt.setString(45, consumerUtil.removeAsciNull(custInfo.getMDCRXOVRCD()));
                pstmt.setString(46, consumerUtil.removeAsciNull(custInfo.getPSEUDOPCPTYPCD()));
                pstmt.setString(47, consumerUtil.removeAsciNull(custInfo.getASGNCOVLVLTYPIND()));
                pstmt.setString(48, consumerUtil.removeAsciNull(custInfo.getMKTSEGTYPCD()));
                pstmt.setString(49, consumerUtil.removeAsciNull(custInfo.getLABTYPCD()));
                pstmt.setString(50, consumerUtil.removeAsciNull(custInfo.getDIFFFAMMBRADRIND()));
                pstmt.setString(51, consumerUtil.removeAsciNull(custInfo.getCUSTCANCRSNTYPCD()));
                pstmt.setString(52, consumerUtil.removeAsciNull(custInfo.getALTIDASGNTYPCD()));
                pstmt.setString(53, consumerUtil.removeAsciNull(custInfo.getELIGANALYST1RACFID()));
                pstmt.setString(54, consumerUtil.removeAsciNull(custInfo.getELIGANALYST2RACFID()));
                pstmt.setString(55, consumerUtil.removeAsciNull(custInfo.getELIGANALYST1NM()));
                pstmt.setString(56, consumerUtil.removeAsciNull(custInfo.getELIGANALYST2NM()));
                pstmt.setString(57, consumerUtil.removeAsciNull(custInfo.getSTRCTSRCCD()));
                pstmt.setString(58, consumerUtil.removeAsciNull(custInfo.getRENMOCD()));
                pstmt.setString(59, consumerUtil.removeAsciNull(custInfo.getRENYRNBR()));
                pstmt.setString(60, consumerUtil.removeAsciNull(custInfo.getCUSTLGLNM()));
                pstmt.setDouble(61, Double.valueOf(custInfo.getCUSTTAXID()));
                pstmt.setInt(62, custInfo.getCUSTLOCNBR());
                pstmt.setString(63, consumerUtil.removeAsciNull(custInfo.getOFCOFSALECD()));
                pstmt.setString(64, consumerUtil.removeAsciNull(custInfo.getUPDTTYPCD()));
                pstmt.setString(65, consumerUtil.removeAsciNull(custInfo.getRACFID()));
                pstmt.setString(66, consumerUtil.removeAsciNull(custInfo.getROWUSERID()));
                pstmt.setString(67, consumerUtil.removeAsciNull(custInfo.getROWSTSCD()));
                pstmt.setTimestamp(68, consumerUtil.timeStampParser( consumerUtil.removeAsciNull(custInfo.getSRCTMSTMP())));
                pstmt.setTimestamp(69, consumerUtil.timeStampParser( consumerUtil.removeAsciNull(custInfo.getROWTMSTMP())));
                pstmt.setString(70, consumerUtil.removeAsciNull(custInfo.getRPTSEGTYPCD()));
                pstmt.setString(71, consumerUtil.removeAsciNull(custInfo.getINTGRCARDTYPCD()));
                pstmt.setString(72, consumerUtil.removeAsciNull(custInfo.getHISTPURGEDAYS()));
                pstmt.setDate(73, consumerUtil.dateParser( consumerUtil.removeAsciNull(custInfo.getOPTUMDT())));
                pstmt.setString(74, consumerUtil.removeAsciNull(custInfo.getORGTYPCD()));
                pstmt.setString(75, consumerUtil.removeAsciNull(custInfo.getMIGSRCCD()));
                pstmt.setString(76, utilityConfig.getSrcSysId());  //src_sys_id
                pstmt.setTimestamp(77, consumerUtil.getsysTimeStamp()); //updt_dttm
                pstmt.setTimestamp(78,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(custInfo.getROWTMSTMP())));


            }
        });


        return batch_count[0];
    }

    public int delete(com.optum.exts.cdb.model.key.CUST_INFO key) throws DataAccessException, SQLException {

        String sql = utilityConfig.getQueryLookup().get("custInfoDel").replace("<SCHEMA>", utilityConfig.getSchema());

        int[] batch_count = jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {

            @Override
            public int getBatchSize() {
                return 1;
            }

            @Override
            public void setValues(PreparedStatement pstmt, int i) throws SQLException {


                pstmt.setString(1, utilityConfig.getPhysicalDelValue());
                pstmt.setTimestamp(2, consumerUtil.getsysTimeStamp());
                pstmt.setString(3,  consumerUtil.removeAsciNull(key.getSRCCD()));
                pstmt.setString(4,  consumerUtil.removeAsciNull(key.getLGCYSRCCUSTID()));


            }
        });


        return batch_count[0];
    }


}


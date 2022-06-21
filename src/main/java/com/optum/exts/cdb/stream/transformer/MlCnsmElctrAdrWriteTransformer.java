package com.optum.exts.cdb.stream.transformer;

import com.optum.exts.cdb.stream.config.UtilityConfig;
import com.optum.exts.cdb.stream.utility.ConsumerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by rgupta59
 */

@Component
public class MlCnsmElctrAdrWriteTransformer {


    private static final Logger log = LoggerFactory.getLogger(MlCnsmElctrAdrWriteTransformer.class);


    @Autowired
    UtilityConfig utilityConfig;
    @Autowired
    ConsumerUtil consumerUtil;
    @Autowired
    JdbcTemplate jdbcTemplate;


    public long insertData(com.optum.exts.cdb.model.key.ML_CNSM_ELCTR_ADR key, com.optum.exts.cdb.model.ML_CNSM_ELCTR_ADR mlCnsmElctrAdr) throws DataAccessException, SQLException {

        if ((mlCnsmElctrAdr.getROWUSERID().toString().trim()).equalsIgnoreCase("LINK/UNLINK") && (mlCnsmElctrAdr.getROWSTSCD().toString().trim()).equalsIgnoreCase("D")) {

            log.info("Skipping Record due to Link/Unlink Delete  :::" + key);
            return 1;
        }


        String sql = utilityConfig.getQueryLookup().get("mlCnsmEltrAdr").replace("<SCHEMA>", utilityConfig.getSchema());
        ;
        int[] batch_count = jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {

            @Override
            public int getBatchSize() {
                return 1;
            }

            @Override
            public void setValues(PreparedStatement pstmt, int i) throws SQLException {

                pstmt.setInt(1, mlCnsmElctrAdr.getPARTNNBR());
                pstmt.setInt(2, mlCnsmElctrAdr.getCNSMID());
                pstmt.setString(3, consumerUtil.removeAsciNull(mlCnsmElctrAdr.getSRCCD()));
                pstmt.setString(4, consumerUtil.removeAsciNull(mlCnsmElctrAdr.getLGCYPOLNBR()));
                pstmt.setString(5, consumerUtil.removeAsciNull(mlCnsmElctrAdr.getLGCYSRCID()));
                pstmt.setString(6, consumerUtil.removeAsciNull(mlCnsmElctrAdr.getELCTRADRTYPCD()));
                pstmt.setString(7, consumerUtil.removeAsciNull(mlCnsmElctrAdr.getELCTRADRTXT()));
                pstmt.setString(8, consumerUtil.removeAsciNull(mlCnsmElctrAdr.getOPTOUTIND()));
                pstmt.setString(9, consumerUtil.removeAsciNull(mlCnsmElctrAdr.getROWSTSCD()));
                pstmt.setString(10, consumerUtil.removeAsciNull(mlCnsmElctrAdr.getUPDTTYPCD()));
                pstmt.setString(11, consumerUtil.removeAsciNull(mlCnsmElctrAdr.getRACFID()));
                pstmt.setString(12, consumerUtil.removeAsciNull(mlCnsmElctrAdr.getROWUSERID()));
                pstmt.setTimestamp(13, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(mlCnsmElctrAdr.getROWTMSTMP().toString())));
                pstmt.setTimestamp(14, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(mlCnsmElctrAdr.getSRCTMSTMP().toString())));
                pstmt.setString(15, utilityConfig.getSrcSysId());
                pstmt.setTimestamp(16, consumerUtil.getsysTimeStamp());
                pstmt.setTimestamp(17, consumerUtil.getsysTimeStamp());

                pstmt.setString(18, consumerUtil.removeAsciNull(mlCnsmElctrAdr.getLGCYPOLNBR()));
                pstmt.setString(19, consumerUtil.removeAsciNull(mlCnsmElctrAdr.getELCTRADRTXT()));
                pstmt.setString(20, consumerUtil.removeAsciNull(mlCnsmElctrAdr.getOPTOUTIND()));
                pstmt.setString(21, consumerUtil.removeAsciNull(mlCnsmElctrAdr.getROWSTSCD()));
                pstmt.setString(22, consumerUtil.removeAsciNull(mlCnsmElctrAdr.getUPDTTYPCD()));
                pstmt.setString(23, consumerUtil.removeAsciNull(mlCnsmElctrAdr.getRACFID()));
                pstmt.setString(24, consumerUtil.removeAsciNull(mlCnsmElctrAdr.getROWUSERID()));
                pstmt.setTimestamp(25, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(mlCnsmElctrAdr.getROWTMSTMP().toString())));
                pstmt.setTimestamp(26, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(mlCnsmElctrAdr.getSRCTMSTMP().toString())));
                pstmt.setString(27, utilityConfig.getSrcSysId());
                pstmt.setTimestamp(28, consumerUtil.getsysTimeStamp());
                pstmt.setInt(29, mlCnsmElctrAdr.getPARTNNBR());
                pstmt.setInt(30, mlCnsmElctrAdr.getCNSMID());
                pstmt.setTimestamp(31,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(mlCnsmElctrAdr.getROWTMSTMP())));


/*



            pstmt.setString(18, consumerUtil.removeAsciNull(mlCnsmElctrAdr.getLGCYPOLNBR()));
            pstmt.setString(19, consumerUtil.removeAsciNull(mlCnsmElctrAdr.getELCTRADRTXT()));
            pstmt.setString(20, consumerUtil.removeAsciNull(mlCnsmElctrAdr.getOPTOUTIND()));
            pstmt.setString(21, consumerUtil.removeAsciNull(mlCnsmElctrAdr.getROWSTSCD()));
            pstmt.setString(22, consumerUtil.removeAsciNull(mlCnsmElctrAdr.getUPDTTYPCD()));
            pstmt.setString(23, consumerUtil.removeAsciNull(mlCnsmElctrAdr.getRACFID()));
            pstmt.setString(24, consumerUtil.removeAsciNull(mlCnsmElctrAdr.getROWUSERID()));
            pstmt.setTimestamp(25, consumerUtil.timeStampParser(mlCnsmElctrAdr.getROWTMSTMP().toString()));
            pstmt.setTimestamp(26, consumerUtil.timeStampParser(mlCnsmElctrAdr.getSRCTMSTMP().toString()));
            pstmt.setString(27, utilityConfig.getSrcSysId());
            pstmt.setTimestamp(28, consumerUtil.getsysTimeStamp());*/


            }
        });

      /*  if (batch_count[0] == 0) {

            standbyTableLoad(key, mlCnsmElctrAdr);
        }*/

        return batch_count[0];
    }


    public int delete(com.optum.exts.cdb.model.key.ML_CNSM_ELCTR_ADR key) {
        //String SQL = "UPDATE systest.CNSM_DTL SET row_sts_cd = ?  WHERE partn_nbr=? and cnsm_id=? and src_cd=? and lgcy_src_id = ? ";

        //String sql1 = utilityConfig.getQueryLookup().get("lCovPrdtDtDel").replace("<SCHEMA>", utilityConfig.getSchema());

        int affectedrows = 0;
        String sql = utilityConfig.getQueryLookup().get("mlCnsmEltrAdrDel").replace("<SCHEMA>", utilityConfig.getSchema());

        int[] batch_count = jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {

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
                pstmt.setString(5,consumerUtil.removeAsciNull( key.getSRCCD()));
                pstmt.setString(6, consumerUtil.removeAsciNull(key.getLGCYSRCID()));
                pstmt.setString(7, consumerUtil.removeAsciNull(key.getELCTRADRTYPCD()));


            }
        });

        return batch_count[0];
    }


    public int standbyTableLoad(com.optum.exts.cdb.model.key.ML_CNSM_ELCTR_ADR key, com.optum.exts.cdb.model.ML_CNSM_ELCTR_ADR mlCnsmElctrAdr) throws DataAccessException, SQLException {


        String sql = utilityConfig.getQueryLookup().get("mlCnsmEltrAdrStandby").replace("<SCHEMA>", utilityConfig.getSchema());
        ;
        int[] batch_count = jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {

            @Override
            public int getBatchSize() {
                return 1;
            }

            @Override
            public void setValues(PreparedStatement pstmt, int i) throws SQLException {

                pstmt.setInt(1, mlCnsmElctrAdr.getPARTNNBR());
                pstmt.setInt(2, mlCnsmElctrAdr.getCNSMID());
                pstmt.setString(3, consumerUtil.removeAsciNull(mlCnsmElctrAdr.getSRCCD()));
                pstmt.setString(4, consumerUtil.removeAsciNull(mlCnsmElctrAdr.getLGCYPOLNBR()));
                pstmt.setString(5, consumerUtil.removeAsciNull(mlCnsmElctrAdr.getLGCYSRCID()));
                pstmt.setString(6, consumerUtil.removeAsciNull(mlCnsmElctrAdr.getELCTRADRTYPCD()));
                pstmt.setString(7, consumerUtil.removeAsciNull(mlCnsmElctrAdr.getELCTRADRTXT()));
                pstmt.setString(8, consumerUtil.removeAsciNull(mlCnsmElctrAdr.getOPTOUTIND()));
                pstmt.setString(9, consumerUtil.removeAsciNull(mlCnsmElctrAdr.getROWSTSCD()));
                pstmt.setString(10, consumerUtil.removeAsciNull(mlCnsmElctrAdr.getUPDTTYPCD()));
                pstmt.setString(11, consumerUtil.removeAsciNull(mlCnsmElctrAdr.getRACFID()));
                pstmt.setString(12, consumerUtil.removeAsciNull(mlCnsmElctrAdr.getROWUSERID()));
                pstmt.setTimestamp(13, consumerUtil.timeStampParser(mlCnsmElctrAdr.getROWTMSTMP().toString()));
                pstmt.setTimestamp(14, consumerUtil.timeStampParser(mlCnsmElctrAdr.getSRCTMSTMP().toString()));
                pstmt.setString(15, utilityConfig.getSrcSysId());
                pstmt.setTimestamp(16, consumerUtil.getsysTimeStamp());
                pstmt.setTimestamp(17, consumerUtil.getsysTimeStamp());
                pstmt.setString(18, consumerUtil.removeAsciNull(mlCnsmElctrAdr.getLGCYPOLNBR()));
                pstmt.setString(19, consumerUtil.removeAsciNull(mlCnsmElctrAdr.getELCTRADRTXT()));
                pstmt.setString(20, consumerUtil.removeAsciNull(mlCnsmElctrAdr.getOPTOUTIND()));
                pstmt.setString(21, consumerUtil.removeAsciNull(mlCnsmElctrAdr.getROWSTSCD()));
                pstmt.setString(22, consumerUtil.removeAsciNull(mlCnsmElctrAdr.getUPDTTYPCD()));
                pstmt.setString(23, consumerUtil.removeAsciNull(mlCnsmElctrAdr.getRACFID()));
                pstmt.setString(24, consumerUtil.removeAsciNull(mlCnsmElctrAdr.getROWUSERID()));
                pstmt.setTimestamp(25, consumerUtil.timeStampParser(mlCnsmElctrAdr.getROWTMSTMP().toString()));
                pstmt.setTimestamp(26, consumerUtil.timeStampParser(mlCnsmElctrAdr.getSRCTMSTMP().toString()));
                pstmt.setString(27, utilityConfig.getSrcSysId());
                pstmt.setTimestamp(28, consumerUtil.getsysTimeStamp());


            }
        });

        return batch_count[0];
    }


}
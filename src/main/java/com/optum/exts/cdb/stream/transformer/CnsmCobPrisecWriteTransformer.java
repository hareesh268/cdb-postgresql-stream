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
public class CnsmCobPrisecWriteTransformer {

    private static final Logger log = LoggerFactory.getLogger(CnsmCobPrisecWriteTransformer.class);

   /* static {
        try {


            Class.forName("org.postgresql.Driver");


        } catch (ClassNotFoundException e) {
            System.err.println("PostgreSQL DataSource unable to load PostgreSQL JDBC Driver");
        }
    }*/

    @Autowired
    UtilityConfig utilityConfig;
    @Autowired
    ConsumerUtil consumerUtil;
    @Autowired
    JdbcTemplate jdbcTemplate;




    public long insertData(com.optum.exts.cdb.model.key.CNSM_COB_PRISEC key, com.optum.exts.cdb.model.CNSM_COB_PRISEC cnsmCobPrisec)throws DataAccessException, SQLException {
     /*   if((cnsmCobPrisec.getROWUSERID().toString().trim()).equalsIgnoreCase("LINK/UNLINK") && (cnsmCobPrisec.getROWSTSCD().toString().trim()).equalsIgnoreCase("D") ) {

            log.info("Skipping Record due to Link/Unlink Delete  :::" + key);
            return 1;
        }*/

//log.info("CNSM_COB_PRISEC processing key "+key.toString());

        String sql = utilityConfig.getQueryLookup().get("cnsmCobPrisec").replace("<SCHEMA>", utilityConfig.getSchema());
        ;
        long id = 0;

        int[] batch_count= jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {

            @Override
            public int getBatchSize() {
                return 1;
            }

            @Override
            public void setValues(PreparedStatement pstmt, int i) throws SQLException {

                pstmt.setInt(1,cnsmCobPrisec.getXREFIDPARTNNBR());
                pstmt.setInt(2,cnsmCobPrisec.getSRCCDBXREFID());
                pstmt.setString(3,consumerUtil.removeAsciNull(cnsmCobPrisec.getSRCCD()));
                pstmt.setString(4,consumerUtil.removeAsciNull(cnsmCobPrisec.getLGCYSRCID()));
                pstmt.setString(5,consumerUtil.removeAsciNull(cnsmCobPrisec.getCOBCOVTYPCD()));
                pstmt.setDate(6,consumerUtil.dateParser(consumerUtil.removeAsciNull(cnsmCobPrisec.getCOBPRISECEFFDT())));
                pstmt.setDate(7,consumerUtil.dateParser(consumerUtil.removeAsciNull(cnsmCobPrisec.getCOBPRISECCANCDT())));
                pstmt.setString(8,consumerUtil.removeAsciNull(cnsmCobPrisec.getCOBPRISECTYPCD()));
                pstmt.setDate(9,consumerUtil.dateParser(consumerUtil.removeAsciNull(cnsmCobPrisec.getOIVERFDT())));
                pstmt.setString(10,consumerUtil.removeAsciNull(cnsmCobPrisec.getUPDTTYPCD()));
                pstmt.setString(11,consumerUtil.removeAsciNull(cnsmCobPrisec.getRACFID()));
                pstmt.setString(12,consumerUtil.removeAsciNull(cnsmCobPrisec.getROWUSERID()));
                pstmt.setTimestamp(13,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmCobPrisec.getSRCTMSTMP())));
                pstmt.setTimestamp(14,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmCobPrisec.getROWTMSTMP())));
                pstmt.setString(15,consumerUtil.removeAsciNull(utilityConfig.getSrcSysId()));
                pstmt.setTimestamp(16,consumerUtil.getsysTimeStamp());
                pstmt.setTimestamp(17,consumerUtil.getsysTimeStamp());
                pstmt.setString(18,"A");


                pstmt.setDate(19,consumerUtil.dateParser(consumerUtil.removeAsciNull(cnsmCobPrisec.getCOBPRISECCANCDT())));
                pstmt.setString(20,consumerUtil.removeAsciNull(consumerUtil.removeAsciNull(cnsmCobPrisec.getCOBPRISECTYPCD())));
                pstmt.setDate(21,consumerUtil.dateParser(consumerUtil.removeAsciNull(cnsmCobPrisec.getOIVERFDT())));
                pstmt.setString(22,consumerUtil.removeAsciNull(cnsmCobPrisec.getUPDTTYPCD()));
                pstmt.setString(23,consumerUtil.removeAsciNull(cnsmCobPrisec.getRACFID()));
                pstmt.setString(24,consumerUtil.removeAsciNull(cnsmCobPrisec.getROWUSERID()));
                pstmt.setTimestamp(25,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmCobPrisec.getSRCTMSTMP())));
                pstmt.setTimestamp(26,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmCobPrisec.getROWTMSTMP())));
                pstmt.setString(27,consumerUtil.removeAsciNull(utilityConfig.getSrcSysId()));
                pstmt.setTimestamp(28,consumerUtil.getsysTimeStamp());
                pstmt.setString(29,"A");
                pstmt.setTimestamp(30,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmCobPrisec.getROWTMSTMP())));


            }});

        return batch_count[0];
    }

    public int delete(com.optum.exts.cdb.model.key.CNSM_COB_PRISEC key ) throws DataAccessException, SQLException{
        //String SQL = "UPDATE systest.CNSM_DTL SET row_sts_cd = ?  WHERE partn_nbr=? and cnsm_id=? and src_cd=? and lgcy_src_id = ? ";

        String sql = utilityConfig.getQueryLookup().get("cnsmCobPrisecDel").replace("<SCHEMA>", utilityConfig.getSchema());

        int affectedrows = 0;

        int[] batch_count= jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {

            @Override
            public int getBatchSize() {
                return 1;
            }

            @Override
            public void setValues(PreparedStatement pstmt, int i) throws SQLException {

                pstmt.setString(1, utilityConfig.getPhysicalDelValue());
                pstmt.setTimestamp(2, consumerUtil.getsysTimeStamp());
                pstmt.setInt(3,key.getXREFIDPARTNNBR());
                pstmt.setInt(4,key.getSRCCDBXREFID());
                pstmt.setString(5,consumerUtil.removeAsciNull(key.getCOBCOVTYPCD()));
                pstmt.setDate(6,consumerUtil.dateParser(consumerUtil.removeAsciNull(key.getCOBPRISECEFFDT())));
        }});

        return batch_count[0];
    }
}
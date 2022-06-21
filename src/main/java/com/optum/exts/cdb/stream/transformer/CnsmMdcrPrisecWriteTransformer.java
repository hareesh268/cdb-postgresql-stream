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

import java.sql.*;

/**
 * Created by rgupta59
 */

@Component
public class CnsmMdcrPrisecWriteTransformer {

    private static final Logger log = LoggerFactory.getLogger(CnsmMdcrPrisecWriteTransformer.class);

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




    public long insertData(com.optum.exts.cdb.model.key.CNSM_MDCR_PRISEC key, com.optum.exts.cdb.model.CNSM_MDCR_PRISEC cnsmMdcrPrisec)throws DataAccessException, SQLException {
     /*   if((cnsmMdcrPrisec.getROWUSERID().toString().trim()).equalsIgnoreCase("LINK/UNLINK") && (cnsmMdcrPrisec.getROWSTSCD().toString().trim()).equalsIgnoreCase("D") ) {

            log.info("Skipping Record due to Link/Unlink Delete  :::" + key);
            return 1;
        }*/

//log.info("CNSM_MDCR_PRISEC processing key "+key.toString());

        String sql = utilityConfig.getQueryLookup().get("cnsmMdcrPrisec").replace("<SCHEMA>", utilityConfig.getSchema());
        ;
        long id = 0;

        int[] batch_count= jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {

            @Override
            public int getBatchSize() {
                return 1;
            }

            @Override
            public void setValues(PreparedStatement pstmt, int i) throws SQLException {

            pstmt.setInt(1, cnsmMdcrPrisec.getXREFIDPARTNNBR());
            pstmt.setInt(2, cnsmMdcrPrisec.getSRCCDBXREFID());
            pstmt.setDate(3, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(cnsmMdcrPrisec.getMDCRPRISECEFFDT().toString())));
            pstmt.setDate(4, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(cnsmMdcrPrisec.getMDCRPRISECCANCDT().toString())));

            pstmt.setString(5, consumerUtil.removeAsciNull(cnsmMdcrPrisec.getMDCRPRISECTYPCD()));

            pstmt.setString(6, consumerUtil.removeAsciNull(cnsmMdcrPrisec.getSRCCD()));
            pstmt.setString(7, consumerUtil.removeAsciNull(cnsmMdcrPrisec.getLGCYSRCID()));

            pstmt.setString(8, consumerUtil.removeAsciNull(cnsmMdcrPrisec.getUPDTTYPCD()));
            pstmt.setString(9, consumerUtil.removeAsciNull(cnsmMdcrPrisec.getRACFID()));
            pstmt.setString(10, consumerUtil.removeAsciNull(cnsmMdcrPrisec.getROWUSERID()));
            pstmt.setTimestamp(11, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmMdcrPrisec.getSRCTMSTMP().toString())));
            pstmt.setTimestamp(12, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmMdcrPrisec.getROWTMSTMP().toString())));
            pstmt.setString(13, utilityConfig.getSrcSysId());
            pstmt.setTimestamp(14, consumerUtil.getsysTimeStamp());
            pstmt.setTimestamp(15, consumerUtil.getsysTimeStamp());
            pstmt.setString(16, "A");

            pstmt.setDate(17, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(cnsmMdcrPrisec.getMDCRPRISECCANCDT().toString())));

            pstmt.setString(18, consumerUtil.removeAsciNull(cnsmMdcrPrisec.getMDCRPRISECTYPCD()));



            pstmt.setString(19, consumerUtil.removeAsciNull(cnsmMdcrPrisec.getUPDTTYPCD()));
            pstmt.setString(20, consumerUtil.removeAsciNull(cnsmMdcrPrisec.getRACFID()));
            pstmt.setString(21, consumerUtil.removeAsciNull(cnsmMdcrPrisec.getROWUSERID()));
            pstmt.setTimestamp(22, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmMdcrPrisec.getSRCTMSTMP().toString())));
            pstmt.setTimestamp(23, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmMdcrPrisec.getROWTMSTMP().toString())));
            pstmt.setString(24, utilityConfig.getSrcSysId());
            pstmt.setTimestamp(25, consumerUtil.getsysTimeStamp());
            pstmt.setString(26, "A");
                pstmt.setTimestamp(27,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmMdcrPrisec.getROWTMSTMP())));



            }});

        return batch_count[0];
    }

    public int delete(com.optum.exts.cdb.model.key.CNSM_MDCR_PRISEC key ) throws DataAccessException, SQLException{
        //String SQL = "UPDATE systest.CNSM_DTL SET row_sts_cd = ?  WHERE partn_nbr=? and cnsm_id=? and src_cd=? and lgcy_src_id = ? ";

        String sql = utilityConfig.getQueryLookup().get("cnsmMdcrPrisecDel").replace("<SCHEMA>", utilityConfig.getSchema());

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
            pstmt.setInt(3, key.getXREFIDPARTNNBR());
            pstmt.setInt(4, key.getSRCCDBXREFID());
            pstmt.setDate(5,  (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(key.getMDCRPRISECEFFDT().toString())));

        }});

        return batch_count[0];
    }
}
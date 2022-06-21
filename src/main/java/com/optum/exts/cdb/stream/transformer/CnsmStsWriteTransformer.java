package com.optum.exts.cdb.stream.transformer;


import com.optum.exts.cdb.model.key.CNSM_STS;
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
public class CnsmStsWriteTransformer {

    private static final Logger log = LoggerFactory.getLogger(CnsmStsWriteTransformer.class);



    @Autowired
    UtilityConfig utilityConfig;
    @Autowired
    ConsumerUtil consumerUtil;
   @Autowired
    JdbcTemplate jdbcTemplate;



    public int insertData(CNSM_STS key, com.optum.exts.cdb.model.CNSM_STS cnsmSts)throws DataAccessException, SQLException {

        // log.info("CNSM_STS processing key :::"+key.toString());

        if((cnsmSts.getROWUSERID().toString().trim()).equalsIgnoreCase("LINK/UNLINK") && (cnsmSts.getROWSTSCD().toString().trim()).equalsIgnoreCase("D") ) {

            log.info("Skipping Record due to Link/Unlink Delete  :::" + key);
            return 1;
        }
        String sql = utilityConfig.getQueryLookup().get("cnsmSts").replace("<SCHEMA>", utilityConfig.getSchema());
        ;//"INSERT INTO stage.cnsm_sts (partn_nbr,cnsm_id,src_cd,lgcy_src_id,cnsm_sts_typ_cd,cnsm_sts_eff_dt,cnsm_sts_canc_dt,sts_verf_dt,src_cdb_xref_id,xref_id_partn_nbr,updt_typ_cd,racf_id,row_user_id,row_sts_cd,src_tmstmp,row_tmstmp,src_sys_id,created_dttm,updt_dttm) VALUES ( ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT (partn_nbr,cnsm_id,src_cd,lgcy_src_id,cnsm_sts_typ_cd,cnsm_sts_eff_dt) DO  UPDATE SET cnsm_sts_canc_dt = ?,sts_verf_dt = ?,src_cdb_xref_id = ?,xref_id_partn_nbr = ?,updt_typ_cd = ?,racf_id = ?,row_user_id = ?,row_sts_cd = ?,src_tmstmp = ?,row_tmstmp = ? ,src_sys_id = ?,updt_dttm = ?";

        int[] batch_count= jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {

            @Override
            public int getBatchSize() {
                return 1;
            }

            @Override
            public void setValues(PreparedStatement pstmt, int i) throws SQLException {

                pstmt.setInt(1, cnsmSts.getPARTNNBR());
                pstmt.setInt(2, cnsmSts.getCNSMID());
                pstmt.setString(3, consumerUtil.removeAsciNull(cnsmSts.getSRCCD()));
                pstmt.setString(4, consumerUtil.removeAsciNull(cnsmSts.getLGCYSRCID()));
                pstmt.setString(5, consumerUtil.removeAsciNull(cnsmSts.getCNSMSTSTYPCD()));
                pstmt.setDate(6, consumerUtil.dateParser(consumerUtil.removeAsciNull(cnsmSts.getCNSMSTSEFFDT().toString())));


                pstmt.setDate(7, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(cnsmSts.getCNSMSTSCANCDT().toString())));
                pstmt.setDate(8, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(cnsmSts.getSTSVERFDT().toString())));
                pstmt.setInt(9, cnsmSts.getSRCCDBXREFID());
                pstmt.setInt(10, cnsmSts.getXREFIDPARTNNBR());
                pstmt.setString(11, consumerUtil.removeAsciNull(cnsmSts.getUPDTTYPCD().toString()));
                pstmt.setString(12, consumerUtil.removeAsciNull(cnsmSts.getRACFID().toString()));
                pstmt.setString(13, consumerUtil.removeAsciNull(cnsmSts.getROWUSERID().toString()));
                pstmt.setString(14, consumerUtil.removeAsciNull(cnsmSts.getROWSTSCD().toString()));
                pstmt.setTimestamp(15, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmSts.getSRCTMSTMP())));
                pstmt.setTimestamp(16, consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmSts.getROWTMSTMP().toString())));


                pstmt.setString(17, utilityConfig.getSrcSysId());
                pstmt.setTimestamp(18, consumerUtil.getsysTimeStamp());
                pstmt.setTimestamp(19, consumerUtil.getsysTimeStamp());


                pstmt.setDate(20, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(cnsmSts.getCNSMSTSCANCDT().toString())));
                pstmt.setDate(21, (Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(cnsmSts.getSTSVERFDT().toString())));
                pstmt.setInt(22, cnsmSts.getCNSMID());
                pstmt.setInt(23, cnsmSts.getPARTNNBR());
                pstmt.setString(24, consumerUtil.removeAsciNull(cnsmSts.getUPDTTYPCD().toString()));
                pstmt.setString(25, consumerUtil.removeAsciNull(cnsmSts.getRACFID().toString()));
                pstmt.setString(26, consumerUtil.removeAsciNull(cnsmSts.getROWUSERID().toString()));
                pstmt.setString(27, consumerUtil.removeAsciNull(cnsmSts.getROWSTSCD().toString()));
                pstmt.setTimestamp(28, consumerUtil.timeStampParser(cnsmSts.getSRCTMSTMP()));
                pstmt.setTimestamp(29, consumerUtil.timeStampParser(cnsmSts.getROWTMSTMP().toString()));

                pstmt.setString(30, utilityConfig.getSrcSysId());


                pstmt.setTimestamp(31, consumerUtil.getsysTimeStamp());
                pstmt.setTimestamp(32,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(cnsmSts.getROWTMSTMP())));


            }});
       // update(key);
        return batch_count[0];
    }
    public int delete(com.optum.exts.cdb.model.key.CNSM_STS key )throws DataAccessException, SQLException {
        //String SQL = "UPDATE systest.CNSM_DTL SET row_sts_cd = ?  WHERE partn_nbr=? and cnsm_id=? and src_cd=? and lgcy_src_id = ? ";

        String sql = utilityConfig.getQueryLookup().get("cnsmStsDel").replace("<SCHEMA>", utilityConfig.getSchema());

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
                pstmt.setString(5,consumerUtil.removeAsciNull( key.getSRCCD()));
                pstmt.setString(6, consumerUtil.removeAsciNull(key.getLGCYSRCID()));
                pstmt.setString(7, consumerUtil.removeAsciNull(key.getCNSMSTSTYPCD()));
                pstmt.setString(8, consumerUtil.removeAsciNull(key.getCNSMSTSEFFDT()));

            }});
        return batch_count[0];
    }
}









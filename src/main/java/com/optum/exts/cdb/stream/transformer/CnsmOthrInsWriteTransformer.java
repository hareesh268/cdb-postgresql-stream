package com.optum.exts.cdb.stream.transformer;

import com.optum.exts.cdb.model.key.CNSM_OTHR_INS;
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
public class CnsmOthrInsWriteTransformer {

    private static final Logger log = LoggerFactory.getLogger(CnsmOthrInsWriteTransformer.class);



    @Autowired
    UtilityConfig utilityConfig;
    @Autowired
    ConsumerUtil consumerUtil;
    @Autowired
    JdbcTemplate jdbcTemplate;




    public long insertData(CNSM_OTHR_INS key, com.optum.exts.cdb.model.CNSM_OTHR_INS CnsmOthrIns) throws DataAccessException, SQLException{

        //log.info("CNSM_OTHR_INS processing key "+key.toString());
       /* String SQL = "INSERT INTO csptest.subscriberAddres () "
                + "VALUES(?,?,?,?,?,?,?,?,?,?)";*/

        //String sql = "INSERT INTO  stage.CNSM_OTHR_INS (partn_nbr,cnsm_id,src_cd,lgcy_src_id,cob_cov_typ_cd,oi_eff_dt,oi_canc_dt,oi_verf_dt,oi_ind,custd_typ_cd,src_updt_typ_cd,updt_rstrc_typ_cd,src_cdb_xref_id,xref_id_partn_nbr,updt_typ_cd,racf_id,row_user_id,row_sts_cd,src_tmstmp,row_tmstmp,src_sys_id,created_dttm,updt_dttm) VALUES ( ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,? ) ON CONFLICT (partn_nbr,cnsm_id,src_cd,lgcy_src_id,cob_cov_typ_cd,oi_eff_dt) DO  UPDATE SET oi_canc_dt= ?,oi_verf_dt= ?,oi_ind= ?,custd_typ_cd= ?,src_updt_typ_cd= ?,updt_rstrc_typ_cd= ?,src_cdb_xref_id= ?,xref_id_partn_nbr= ?,updt_typ_cd= ?,racf_id= ?,row_user_id= ?,row_sts_cd= ?,src_tmstmp= ?,row_tmstmp= ?,src_sys_id= ?,created_dttm= ?,updt_dttm= ?";
        if((CnsmOthrIns.getROWUSERID().toString().trim()).equalsIgnoreCase("LINK/UNLINK") && (CnsmOthrIns.getROWSTSCD().toString().trim()).equalsIgnoreCase("D") ) {

            log.info("Skipping Record due to Link/Unlink Delete  :::" + key);
            return 1;
        }

        String sql = utilityConfig.getQueryLookup().get("cnsmOthrIns").replace("<SCHEMA>", utilityConfig.getSchema());

        int[] batch_count= jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {

            @Override
            public int getBatchSize() {
                return 1;
            }

            @Override
            public void setValues(PreparedStatement pstmt, int i) throws SQLException {
            pstmt.setInt( 1,CnsmOthrIns.getPARTNNBR());
            pstmt.setInt(2,CnsmOthrIns.getCNSMID());
            pstmt.setString(3,consumerUtil.removeAsciNull(CnsmOthrIns.getSRCCD()));
            pstmt.setString(4,consumerUtil.removeAsciNull(CnsmOthrIns.getLGCYSRCID()));
            pstmt.setString(5,consumerUtil.removeAsciNull(CnsmOthrIns.getCOBCOVTYPCD()));
            pstmt.setDate( 6,(Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(CnsmOthrIns.getOIEFFDT().toString())));
            pstmt.setDate( 7,(Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(CnsmOthrIns.getOICANCDT().toString())));
            pstmt.setDate( 8,(Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(CnsmOthrIns.getOIVERFDT().toString())));
            pstmt.setString(9,consumerUtil.removeAsciNull(CnsmOthrIns.getOIIND()));
            pstmt.setString(10,consumerUtil.removeAsciNull(CnsmOthrIns.getCUSTDTYPCD()));
            pstmt.setString(11,consumerUtil.removeAsciNull(CnsmOthrIns.getSRCUPDTTYPCD()));
            pstmt.setString(12,consumerUtil.removeAsciNull(CnsmOthrIns.getUPDTRSTRCTYPCD()));
            pstmt.setInt( 13,CnsmOthrIns.getSRCCDBXREFID());
            pstmt.setInt( 14,CnsmOthrIns.getXREFIDPARTNNBR());
            pstmt.setString(15,consumerUtil.removeAsciNull(CnsmOthrIns.getUPDTTYPCD()));
            pstmt.setString(16,consumerUtil.removeAsciNull(CnsmOthrIns.getRACFID()));
            pstmt.setString(17,consumerUtil.removeAsciNull(CnsmOthrIns.getROWUSERID()));
            pstmt.setString(18,consumerUtil.removeAsciNull(CnsmOthrIns.getROWSTSCD()));
            pstmt.setTimestamp(19,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(CnsmOthrIns.getSRCTMSTMP().toString())));
            pstmt.setTimestamp(20,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(CnsmOthrIns.getROWTMSTMP().toString())));
            pstmt.setString(21, utilityConfig.getSrcSysId());
            pstmt.setTimestamp( 22, consumerUtil.getsysTimeStamp());
            pstmt.setTimestamp( 23, consumerUtil.getsysTimeStamp());

            pstmt.setDate( 24,(Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(CnsmOthrIns.getOICANCDT().toString())));
            pstmt.setDate( 25,(Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(CnsmOthrIns.getOIVERFDT().toString())));
            pstmt.setString(26,consumerUtil.removeAsciNull(CnsmOthrIns.getOIIND()));
            pstmt.setString(27,consumerUtil.removeAsciNull(CnsmOthrIns.getCUSTDTYPCD()));
            pstmt.setString(28,consumerUtil.removeAsciNull(CnsmOthrIns.getSRCUPDTTYPCD()));
            pstmt.setString(29,consumerUtil.removeAsciNull(CnsmOthrIns.getUPDTRSTRCTYPCD()));
            pstmt.setInt( 30,CnsmOthrIns.getCNSMID());
            pstmt.setInt( 31,CnsmOthrIns.getPARTNNBR());
            pstmt.setString(32,consumerUtil.removeAsciNull(CnsmOthrIns.getUPDTTYPCD()));
            pstmt.setString(33,consumerUtil.removeAsciNull(CnsmOthrIns.getRACFID()));
            pstmt.setString(34,consumerUtil.removeAsciNull(CnsmOthrIns.getROWUSERID()));
            pstmt.setString(35,consumerUtil.removeAsciNull(CnsmOthrIns.getROWSTSCD()));
            pstmt.setTimestamp(36,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(CnsmOthrIns.getSRCTMSTMP().toString())));
            pstmt.setTimestamp(37,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(CnsmOthrIns.getROWTMSTMP().toString())));
            pstmt.setString(38, utilityConfig.getSrcSysId());
            pstmt.setTimestamp( 39, consumerUtil.getsysTimeStamp());
            pstmt.setTimestamp( 40, consumerUtil.getsysTimeStamp());
                pstmt.setTimestamp(41,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(CnsmOthrIns.getROWTMSTMP())));


            }});

        return batch_count[0];
    }

    public int delete(CNSM_OTHR_INS key ) throws DataAccessException, SQLException{
        //String SQL = "UPDATE systest.CNSM_OTHR_INS SET row_sts_cd = ?  WHERE partn_nbr=? and cnsm_id=? and src_cd=? and lgcy_src_id = ? ";

        String sql = utilityConfig.getQueryLookup().get("cnsmOthrInsDel").replace("<SCHEMA>", utilityConfig.getSchema());

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
            pstmt.setInt( 3,key.getPARTNNBR());
            pstmt.setInt(4,key.getCNSMID());
            pstmt.setString(5,consumerUtil.removeAsciNull(key.getSRCCD()));
            pstmt.setString(6,consumerUtil.removeAsciNull(key.getLGCYSRCID()));
            pstmt.setString(7,consumerUtil.removeAsciNull(key.getCOBCOVTYPCD()));
            pstmt.setDate( 8,(Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(key.getOIEFFDT().toString())));

        }});

        return batch_count[0];
    }
}
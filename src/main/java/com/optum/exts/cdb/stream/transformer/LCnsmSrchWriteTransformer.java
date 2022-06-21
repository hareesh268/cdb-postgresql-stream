package com.optum.exts.cdb.stream.transformer;

import com.optum.exts.cdb.stream.config.UtilityConfig;
import com.optum.exts.cdb.stream.utility.ConsumerUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;



import java.sql.*;

/**
 * Created by rgupta59
 */


@Configuration
@ConfigurationProperties("spring.datasource")
public class    LCnsmSrchWriteTransformer {

    private static final Logger log = LoggerFactory.getLogger(LCnsmSrchWriteTransformer.class);


    @Autowired
    UtilityConfig utilityConfig;
    @Autowired
    ConsumerUtil consumerUtil;
   @Autowired
    private JdbcTemplate jdbcTemplate;





    public int insertData(com.optum.exts.cdb.model.key.L_CNSM_SRCH key, com.optum.exts.cdb.model.L_CNSM_SRCH lcnsmSrch)throws DataAccessException, SQLException {

//log.info("L_CNSM_SRCH processing key "+key.toString());

        String sql = utilityConfig.getQueryLookup().get("lCnsmSrch").replace("<SCHEMA>",utilityConfig.getSchema());;


        String id = "";

        if((lcnsmSrch.getROWUSERID().toString().trim()).equalsIgnoreCase("LINK/UNLINK") && (lcnsmSrch.getROWSTSCD().toString().trim()).equalsIgnoreCase("D") ) {

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



                pstmt.setString( 1,consumerUtil.removeAsciNull(lcnsmSrch.getUPDTTYPCD()));
                pstmt.setDate( 2,(Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lcnsmSrch.getBTHDT().toString())));
                pstmt.setDate( 3,(Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lcnsmSrch.getEESTRTDT().toString())));
                pstmt.setDate( 4,(Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lcnsmSrch.getTOPSORIGCOVEFFDT().toString())));
                pstmt.setInt( 5,lcnsmSrch.getPARTNNBR());
                pstmt.setInt( 6,lcnsmSrch.getSECLVLCD());
                pstmt.setInt( 7,lcnsmSrch.getFAMPARTNNBR());
                pstmt.setInt( 8,lcnsmSrch.getXREFIDPARTNNBR());
                pstmt.setInt( 9,lcnsmSrch.getWRKHRNBR());
                pstmt.setInt( 10,lcnsmSrch.getCNSMID());
                pstmt.setInt( 11,lcnsmSrch.getFAMID());
                pstmt.setInt( 12,lcnsmSrch.getSRCCDBXREFID());
                pstmt.setInt( 13,lcnsmSrch.getPRFLID());
                pstmt.setDouble( 14,Double.valueOf(consumerUtil.removeAsciNull(lcnsmSrch.getSLRYAMT())));
                pstmt.setTimestamp( 15,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(lcnsmSrch.getSRCTMSTMP().toString())));
                pstmt.setTimestamp( 16,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(lcnsmSrch.getROWTMSTMP().toString())));
                pstmt.setTimestamp( 17, consumerUtil.getsysTimeStamp());
                pstmt.setString( 18,consumerUtil.removeAsciNull(lcnsmSrch.getSRCCD()));
                pstmt.setString( 19,consumerUtil.removeAsciNull(lcnsmSrch.getLGCYSRCID()));
                pstmt.setString( 20,consumerUtil.removeAsciNull(lcnsmSrch.getLGCYPOLNBR()));
                pstmt.setString( 21,consumerUtil.removeAsciNull(lcnsmSrch.getSOCSECNBR()));
                pstmt.setString( 22,consumerUtil.removeAsciNull(lcnsmSrch.getLSTNM()));
                pstmt.setString( 23,consumerUtil.removeAsciNull(lcnsmSrch.getFSTNM()));
                pstmt.setString( 24,consumerUtil.removeAsciNull(lcnsmSrch.getMIDLINITTXT()));
                pstmt.setString( 25,consumerUtil.removeAsciNull(lcnsmSrch.getMIDLNM()));
                pstmt.setString( 26,consumerUtil.removeAsciNull(lcnsmSrch.getNMGENSUFXTYPCD()));
                pstmt.setString( 27,consumerUtil.removeAsciNull(lcnsmSrch.getSLTNTYPCD()));
                pstmt.setString( 28,consumerUtil.removeAsciNull(lcnsmSrch.getGDRTYPCD()));
                pstmt.setString( 29,consumerUtil.removeAsciNull(lcnsmSrch.getPROTHLTHINFOIND()));
                pstmt.setString( 30,consumerUtil.removeAsciNull(lcnsmSrch.getPSTCD()));
                pstmt.setString( 31,consumerUtil.removeAsciNull(lcnsmSrch.getSTCD()));
                pstmt.setString( 32,consumerUtil.removeAsciNull(lcnsmSrch.getEESTSTYPCD()));
                pstmt.setString( 33,consumerUtil.removeAsciNull(lcnsmSrch.getSPCLPROCHNDLCD()));
                pstmt.setString( 34,consumerUtil.removeAsciNull(lcnsmSrch.getLGCYSBSCRID()));
                pstmt.setString( 35,consumerUtil.removeAsciNull(lcnsmSrch.getLGCYMBRID()));
                pstmt.setString( 36,consumerUtil.removeAsciNull(lcnsmSrch.getLGCYALTMBRID()));
                pstmt.setString( 37,consumerUtil.removeAsciNull(lcnsmSrch.getHCACNBR()));
                pstmt.setString( 38,consumerUtil.removeAsciNull(lcnsmSrch.getSBSCRRELTYPCD()));
                pstmt.setString( 39,consumerUtil.removeAsciNull(lcnsmSrch.getTOPSRELCD()));
                pstmt.setString( 40,consumerUtil.removeAsciNull(lcnsmSrch.getROWSTSCD()));
                pstmt.setString( 41,consumerUtil.removeAsciNull(lcnsmSrch.getLGCYSRCFAMID()));
                pstmt.setString( 42,consumerUtil.removeAsciNull(lcnsmSrch.getROWUSERID()));
                pstmt.setString( 43,consumerUtil.removeAsciNull(lcnsmSrch.getDEPNCD()));
                pstmt.setString( 44,consumerUtil.removeAsciNull(lcnsmSrch.getLGCYCLSSID()));
                pstmt.setString( 45,consumerUtil.removeAsciNull(lcnsmSrch.getSLRYTYPCD()));
                pstmt.setString( 46,consumerUtil.removeAsciNull(lcnsmSrch.getTBCCUSEIND()));
                pstmt.setString( 47,consumerUtil.removeAsciNull(lcnsmSrch.getLGCYCUSTNBR()));
                pstmt.setString( 48,consumerUtil.removeAsciNull(lcnsmSrch.getTOPSSEQNBR()));
                pstmt.setString( 49,consumerUtil.removeAsciNull(lcnsmSrch.getMRTLSTSTYPCD()));
                pstmt.setString( 50,consumerUtil.removeAsciNull(lcnsmSrch.getPRIMEDEPNCD()));
                pstmt.setString( 51,consumerUtil.removeAsciNull(lcnsmSrch.getQMCSOIND()));
                pstmt.setString( 52,consumerUtil.removeAsciNull(lcnsmSrch.getRACFID()));
                pstmt.setString( 53,consumerUtil.removeAsciNull(lcnsmSrch.getEMPMTCLSS1TYPCD()));
                pstmt.setString( 54,consumerUtil.removeAsciNull(lcnsmSrch.getEMPMTCLSS2TYPCD()));
                pstmt.setString( 55,consumerUtil.removeAsciNull(lcnsmSrch.getEMPMTCLSS3TYPCD()));
                pstmt.setString( 56,consumerUtil.removeAsciNull(lcnsmSrch.getDEPTNBR()));
                pstmt.setString( 57,consumerUtil.removeAsciNull(lcnsmSrch.getDIVNBR()));
                pstmt.setString( 58,consumerUtil.removeAsciNull(lcnsmSrch.getENRLRSNTYPCD()));
                pstmt.setString( 59,consumerUtil.removeAsciNull(lcnsmSrch.getORGTYPCD()));
                pstmt.setString(60, utilityConfig.getSrcSysId());
                pstmt.setTimestamp( 61, consumerUtil.getsysTimeStamp());

                pstmt.setString( 62,consumerUtil.removeAsciNull(lcnsmSrch.getUPDTTYPCD()));
                pstmt.setDate( 63,(Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lcnsmSrch.getBTHDT().toString())));
                pstmt.setDate( 64,(Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lcnsmSrch.getEESTRTDT().toString())));
                pstmt.setDate( 65,(Date) consumerUtil.dateParser(consumerUtil.removeAsciNull(lcnsmSrch.getTOPSORIGCOVEFFDT().toString())));
                pstmt.setInt( 66,lcnsmSrch.getSECLVLCD());
                pstmt.setInt( 67,lcnsmSrch.getFAMPARTNNBR());
                pstmt.setInt( 68,lcnsmSrch.getPARTNNBR());
                pstmt.setInt( 69,lcnsmSrch.getWRKHRNBR());
                pstmt.setInt( 70,lcnsmSrch.getFAMID());
                pstmt.setInt( 71,lcnsmSrch.getCNSMID());
                pstmt.setInt( 72,lcnsmSrch.getPRFLID());
                pstmt.setDouble( 73,Double.valueOf(consumerUtil.removeAsciNull(lcnsmSrch.getSLRYAMT())));
                pstmt.setTimestamp( 74,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(lcnsmSrch.getSRCTMSTMP().toString())));
                pstmt.setTimestamp( 75,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(lcnsmSrch.getROWTMSTMP().toString())));
                pstmt.setString( 76,consumerUtil.removeAsciNull(lcnsmSrch.getLGCYPOLNBR()));
                pstmt.setString( 77,consumerUtil.removeAsciNull(lcnsmSrch.getSOCSECNBR()));
                pstmt.setString( 78,consumerUtil.removeAsciNull(lcnsmSrch.getLSTNM()));
                pstmt.setString( 79,consumerUtil.removeAsciNull(lcnsmSrch.getFSTNM()));
                pstmt.setString( 80,consumerUtil.removeAsciNull(lcnsmSrch.getMIDLINITTXT()));
                pstmt.setString( 81,consumerUtil.removeAsciNull(lcnsmSrch.getMIDLNM()));
                pstmt.setString( 82,consumerUtil.removeAsciNull(lcnsmSrch.getNMGENSUFXTYPCD()));
                pstmt.setString( 83,consumerUtil.removeAsciNull(lcnsmSrch.getSLTNTYPCD()));
                pstmt.setString( 84,consumerUtil.removeAsciNull(lcnsmSrch.getGDRTYPCD()));
                pstmt.setString( 85,consumerUtil.removeAsciNull(lcnsmSrch.getPROTHLTHINFOIND()));
                pstmt.setString( 86,consumerUtil.removeAsciNull(lcnsmSrch.getPSTCD()));
                pstmt.setString( 87,consumerUtil.removeAsciNull(lcnsmSrch.getSTCD()));
                pstmt.setString( 88,consumerUtil.removeAsciNull(lcnsmSrch.getEESTSTYPCD()));
                pstmt.setString( 89,consumerUtil.removeAsciNull(lcnsmSrch.getSPCLPROCHNDLCD()));
                pstmt.setString( 90,consumerUtil.removeAsciNull(lcnsmSrch.getLGCYSBSCRID()));
                pstmt.setString( 91,consumerUtil.removeAsciNull(lcnsmSrch.getLGCYMBRID()));
                pstmt.setString( 92,consumerUtil.removeAsciNull(lcnsmSrch.getLGCYALTMBRID()));
                pstmt.setString( 93,consumerUtil.removeAsciNull(lcnsmSrch.getHCACNBR()));
                pstmt.setString( 94,consumerUtil.removeAsciNull(lcnsmSrch.getSBSCRRELTYPCD()));
                pstmt.setString( 95,consumerUtil.removeAsciNull(lcnsmSrch.getTOPSRELCD()));
                pstmt.setString( 96,consumerUtil.removeAsciNull(lcnsmSrch.getROWSTSCD()));
                pstmt.setString( 97,consumerUtil.removeAsciNull(lcnsmSrch.getLGCYSRCFAMID()));
                pstmt.setString( 98,consumerUtil.removeAsciNull(lcnsmSrch.getROWUSERID()));
                pstmt.setString( 99,consumerUtil.removeAsciNull(lcnsmSrch.getDEPNCD()));
                pstmt.setString( 100,consumerUtil.removeAsciNull(lcnsmSrch.getLGCYCLSSID()));
                pstmt.setString( 101,consumerUtil.removeAsciNull(lcnsmSrch.getSLRYTYPCD()));
                pstmt.setString( 102,consumerUtil.removeAsciNull(lcnsmSrch.getTBCCUSEIND()));
                pstmt.setString( 103,consumerUtil.removeAsciNull(lcnsmSrch.getLGCYCUSTNBR()));
                pstmt.setString( 104,consumerUtil.removeAsciNull(lcnsmSrch.getTOPSSEQNBR()));
                pstmt.setString( 105,consumerUtil.removeAsciNull(lcnsmSrch.getMRTLSTSTYPCD()));
                pstmt.setString( 106,consumerUtil.removeAsciNull(lcnsmSrch.getPRIMEDEPNCD()));
                pstmt.setString( 107,consumerUtil.removeAsciNull(lcnsmSrch.getQMCSOIND()));
                pstmt.setString( 108,consumerUtil.removeAsciNull(lcnsmSrch.getRACFID()));
                pstmt.setString( 109,consumerUtil.removeAsciNull(lcnsmSrch.getEMPMTCLSS1TYPCD()));
                pstmt.setString( 110,consumerUtil.removeAsciNull(lcnsmSrch.getEMPMTCLSS2TYPCD()));
                pstmt.setString( 111,consumerUtil.removeAsciNull(lcnsmSrch.getEMPMTCLSS3TYPCD()));
                pstmt.setString( 112,consumerUtil.removeAsciNull(lcnsmSrch.getDEPTNBR()));
                pstmt.setString( 113,consumerUtil.removeAsciNull(lcnsmSrch.getDIVNBR()));
                pstmt.setString( 114,consumerUtil.removeAsciNull(lcnsmSrch.getENRLRSNTYPCD()));
                pstmt.setString( 115,consumerUtil.removeAsciNull(lcnsmSrch.getORGTYPCD()));
                pstmt.setString(116, utilityConfig.getSrcSysId());
                pstmt.setTimestamp( 117, consumerUtil.getsysTimeStamp());
                pstmt.setTimestamp( 118,consumerUtil.timeStampParser(consumerUtil.removeAsciNull(lcnsmSrch.getROWTMSTMP().toString())));











            }});



        return batch_count[0];
    }

    public int delete(com.optum.exts.cdb.model.key.L_CNSM_SRCH  key ) throws DataAccessException, SQLException{
        //String SQL = "UPDATE systest.CNSM_DTL SET row_sts_cd = ?  WHERE partn_nbr=? and cnsm_id=? and src_cd=? and lgcy_src_id = ? ";

        String sql = utilityConfig.getQueryLookup().get("lCnsmSrchDel").replace("<SCHEMA>", utilityConfig.getSchema());

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
                pstmt.setInt(3, key.getPARTNNBR());
                pstmt.setInt(4, key.getCNSMID());
                pstmt.setString(5, consumerUtil.removeAsciNull(key.getSRCCD()));
                pstmt.setString(6, consumerUtil.removeAsciNull(key.getLGCYSRCID()));









            }});


        return batch_count[0];
    }


}
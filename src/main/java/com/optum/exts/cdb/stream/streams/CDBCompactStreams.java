package com.optum.exts.cdb.stream.streams;


import com.optum.exts.cdb.model.key.*;
import com.optum.exts.cdb.stream.config.TopicConfig;
import com.optum.exts.cdb.stream.config.UtilityConfig;
import com.optum.exts.cdb.stream.transformer.*;
import com.optum.exts.cdb.stream.utility.ConsumerUtil;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import javax.validation.constraints.Max;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by rgupta59
 */
@Configuration
@ConfigurationProperties(prefix = "streams.member")
//@Profile({"local", "member"})
public class CDBCompactStreams {

    private static final Logger log = LoggerFactory.getLogger(CDBCompactStreams.class);
    SimpleDateFormat sdf =    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");



    @Autowired
    CnsmStsWriteTransformer cnsmStsWriteTransformer;

    @Autowired
    LCnsmSrchWriteTransformer lCnsmSrchWriteTransformer;

    @Autowired
    LCovPrdtWriteTransformer lCovPrdtWriteTransformer;

    @Autowired
    MlCnsmAdrWriteTransformer mlCnsmAdrWriteTransformer;


    @Autowired
    MlCnsmTelWriteTransformer mlCnsmTelWriteTransformer;

    @Autowired
    MlCnsmElctrAdrWriteTransformer mlCnsmElctrAdrWriteTransformer;

    @Autowired
    LCovPrdtPcpWriteTransformer lCovPrdtPcpWriteTransformer;

    @Autowired
    CnsmDtlWriteTransformer cnsmDtlWriteTransformer;

    @Autowired
    LHtlSrvDtWriteTransformer lHtlSrvDtWriteTransformer;

    @Autowired
    LLfDisPrdtDtWriteTransformer lLfDisPrdtDtWriteTransformer;

    @Autowired
    CdbSecurityWriteTransformer cdbSecurityWriteTransformer;

    @Autowired
    CnsmMdcrEnrlWriteTransformer cnsmMdcrEnrlWriteTransformer;

    @Autowired
    CnsmMdcrPrisecWriteTransformer cnsmMdcrPrisecWriteTransformer;

    @Autowired
    MlCnsmXrefWriteTransformer mlCnsmXrefWriteTransformer;

    @Autowired
    CovLvlTypeWriteTransformer covLvlTypeWriteTransformer;

    @Autowired
    CnsmOthrInsWriteTransformer cnsmOthrInsWriteTransformer;

    @Autowired
    CnsmMdcrEntlWriteTransformer cnsmMdcrEntlWriteTransformer;

    @Autowired
    CnsmAuthRepWriteTransformer cnsmAuthRepWriteTransformer;

    @Autowired
    CnsmCalWriteTransformer cnsmCalWriteTransformer;

    @Autowired
    CnsmCobPrimacyWriteTransformer cnsmCobPrimacyWriteTransformer;

    @Autowired
    CnsmCobPrisecWriteTransformer cnsmCobPrisecWriteTransformer;

    @Autowired
    CnsmCovCustDefnFldWriteTransformer cnsmCovCustDefnFldWriteTransformer;

    @Autowired
    CnsmCustDefnFldWriteTransformer cnsmCustDefnFldWriteTransformer;

    @Autowired
    CnsmEftWriteTransformer cnsmEftWriteTransformer;

    @Autowired
    CnsmMdcrEligWriteTransformer cnmsMdcrEligWriteTransformer;

    @Autowired
    CnsmMdcrPrimacyWriteTransformer cnmsMdcrPrimacyWriteTransformer;

    @Autowired
    CnsmPrxstCondWriteTransformer cnmsPrxstCondWriteTransformer;

    @Autowired
    CnsmSlryBasDedOopWriteTransformer cnsmSlryBasDedOopWriteTransformer;

    @Autowired
    CovInfoWriteTransformer covInfoWriteTransformerWriteTransformer;

    @Autowired
    CustInfoWriteTransformer custInfoWriteTransformer;

    @Autowired
    PlnBenSetWriteTransformer plnBenSetWriteTransformer;

    @Autowired
    PlnBenSetDetWriteTransformer plnBenSetDetWriteTransformer;

    @Autowired
    PolInfoWriteTransformer polInfoWriteTransformer;

    @Autowired
    RxBenSetDetWriteTransformer rxBenSetDetWriteTransformer;

    @Autowired
    TopicConfig topicConfig;

    @Autowired
    UtilityConfig utilityConfig;

    @Autowired
    ConsumerUtil consumerUtil;







    public CnsmStsWriteTransformer getCnsmStsWriteTransformer() {
        return cnsmStsWriteTransformer;
    }

    public void setCnsmStsWriteTransformer(CnsmStsWriteTransformer cnsmStsWriteTransformer) {
        this.cnsmStsWriteTransformer = cnsmStsWriteTransformer;
    }


    @Bean
    RetryTemplate retryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();

        /*FixedBackOffPolicy fixedBackOffPolicy = new FixedBackOffPolicy();
        fixedBackOffPolicy.setBackOffPeriod(1000*60);
        retryTemplate.setBackOffPolicy(fixedBackOffPolicy);
*/
        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
        retryPolicy.setMaxAttempts(10);

        retryTemplate.setRetryPolicy(retryPolicy);
        retryTemplate.setThrowLastExceptionOnExhausted(true);

        return retryTemplate;
    }

    @Bean
    public KStream<L_CNSM_SRCH, com.optum.exts.cdb.model.L_CNSM_SRCH> lCNSMSrchStream(StreamsBuilder builder) {

        if (!consumerUtil.deploymentFlag("L_CNSM_SRCH")) {

            return null;
        }

        log.info("::::::::::::::::::::::::::::::::::::::::L_CNSM_SRCH Consumer is Activated ::::::::::::::::::::::::::::");
        KStream<L_CNSM_SRCH, com.optum.exts.cdb.model.L_CNSM_SRCH>
                memberKStream = builder.stream(topicConfig.getLcnsmSrch());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("L_CNSM_SRCH Consuming key  :::" + key.toString());

                try {
                    if (value != null)
                    {    lCnsmSrchWriteTransformer.insertData(key, value);
                        log.info("L_CNSM_SRCH written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());




                    } else {

                        lCnsmSrchWriteTransformer.delete(key);
                        log.info("L_CNSM_SRCH physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                    }
                }
                catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<L_CNSM_SRCH, com.optum.exts.cdb.model.L_CNSM_SRCH>, Exception>() {
                            @Override
                            public KeyValue<L_CNSM_SRCH, com.optum.exts.cdb.model.L_CNSM_SRCH> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (value != null) {
                                    lCnsmSrchWriteTransformer.insertData(key, value);
                                    log.info("L_CNSM_SRCH written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                } else {
                                    lCnsmSrchWriteTransformer.delete(key);
                                    log.info("L_CNSM_SRCH physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                }
                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {

                        log.error("SQLException in Table L_CNSM_SRCH :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                        log.error("SQLException in Table L_CNSM_SRCH :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());

                        log.error("Exception",throwable);
                        //throw new RuntimeException("");
                        log.error("Restarting L_CNSM_SRCH POD"); System.exit(1);
                        //log.error("Exception",throwable);
                    }
                }


                return KeyValue.pair(key,value);
            }).transform(new MetadataTransformer());
        } catch (Exception e) {
            log.error("Exception in Table L_CNSM_SRCH ::" + memberKStream.transform(new MetadataTransformer()));
            e.printStackTrace();
            log.error("Restarting L_CNSM_SRCH POD"); System.exit(1);
        }


        return memberKStream;
    }


    @Bean
        public KStream<L_COV_PRDT_DT, com.optum.exts.cdb.model.L_COV_PRDT_DT> lCovPrdtDtStream(StreamsBuilder builder) {

        if (!consumerUtil.deploymentFlag("L_COV_PRDT_DT")) {

            return null;
        }

        log.info("::::::::::::::::::::::::::::::::::::::::L_COV_PRDT_DT Consumer is Activated ::::::::::::::::::::::::::::");

        KStream<L_COV_PRDT_DT, com.optum.exts.cdb.model.L_COV_PRDT_DT>
                memberKStream = builder.stream(topicConfig.getLcovPrdtdt());
    //log.info("running running ++++++++++++++++++");
        try {
            memberKStream.map((key, value) -> {


                final int[] retryCount = {0};
                int code= 0 ;

                log.info("L_COV_PRDT_DT Consuming key +++ ::time::::"+  sdf.format(new Date()) + ":::::::" + key   );
                try {
                    if (value != null && value.getROWSTSCD().trim().equalsIgnoreCase("A")) {

                        code =  lCovPrdtWriteTransformer.insertData(key, value);

                       if (code == 0)


                       {  log.info("L_COV_PRDT_DT not written key  +++ ::time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                           L_COV_PRDT_DT_MEM_CNT_KEY prdtKey = L_COV_PRDT_DT_MEM_CNT_KEY.newBuilder()

                                   .build();
                           return KeyValue.pair(prdtKey, null);

                       }
                        log.info("L_COV_PRDT_DT written key  +++ ::time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                        L_COV_PRDT_DT_MEM_CNT_KEY prdtKey = L_COV_PRDT_DT_MEM_CNT_KEY.newBuilder()
                                .setPARTNNBR(key.getPARTNNBR())
                                .setCNSMID(key.getCNSMID())
                                .setCOVEFFDT(key.getCOVEFFDT())
                                .setCOVTYPCD(key.getCOVTYPCD())
                                .setLGCYPOLNBR(key.getLGCYPOLNBR())
                                .setSRCCD(key.getSRCCD())
                                .setLGCYSRCID(key.getLGCYSRCID())
                                .build();




                        return KeyValue.pair(prdtKey, prdtKey);
                    } else if (value != null && value.getROWSTSCD().trim().equalsIgnoreCase("D")) {

                        code= lCovPrdtWriteTransformer.insertData(key, value);


                        if (code == 0)
                        { log.info("L_COV_PRDT_DT not written[Logical DELETES] key ::time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                            L_COV_PRDT_DT_MEM_CNT_KEY prdtKey = L_COV_PRDT_DT_MEM_CNT_KEY.newBuilder()

                                    .build();
                            return KeyValue.pair(prdtKey, null);

                        }
                        log.info("L_COV_PRDT_DT written[Logical DELETES] key ::time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                        L_COV_PRDT_DT_MEM_CNT_KEY prdtKey = L_COV_PRDT_DT_MEM_CNT_KEY.newBuilder()
                                .setPARTNNBR(key.getPARTNNBR())
                                .setCNSMID(key.getCNSMID())
                                .setCOVEFFDT(key.getCOVEFFDT())
                                .setCOVTYPCD(key.getCOVTYPCD())
                                .setLGCYPOLNBR(key.getLGCYPOLNBR())
                                .setSRCCD(key.getSRCCD())
                                .setLGCYSRCID(key.getLGCYSRCID())
                                .build();



                        return KeyValue.pair(prdtKey, null);
                    } else {

                       code =  lCovPrdtWriteTransformer.delete(key);
                        if (code == 0)
                        {
                            log.info("L_COV_PRDT_DT  not written physical delete  +++ ::time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                            L_COV_PRDT_DT_MEM_CNT_KEY prdtKey = L_COV_PRDT_DT_MEM_CNT_KEY.newBuilder()

                                    .build();
                            return KeyValue.pair(prdtKey, null);

                        }
                        log.info("L_COV_PRDT_DT written physical delete  +++ ::time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                        L_COV_PRDT_DT_MEM_CNT_KEY prdtKey = L_COV_PRDT_DT_MEM_CNT_KEY.newBuilder()
                                .setPARTNNBR(key.getPARTNNBR())
                                .setCNSMID(key.getCNSMID())
                                .setCOVEFFDT(key.getCOVEFFDT())
                                .setCOVTYPCD(key.getCOVTYPCD())
                                .setLGCYPOLNBR(key.getLGCYPOLNBR())
                                .setSRCCD(key.getSRCCD())
                                .setLGCYSRCID(key.getLGCYSRCID())
                                .build();



                        return KeyValue.pair(prdtKey, null);

                    }

                } catch (Exception e) {

                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<L_COV_PRDT_DT_MEM_CNT_KEY, L_COV_PRDT_DT_MEM_CNT_KEY>, Exception>() {
                            @Override
                            public KeyValue<L_COV_PRDT_DT_MEM_CNT_KEY, L_COV_PRDT_DT_MEM_CNT_KEY> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                int code= 0 ;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (value != null && value.getROWSTSCD().trim().equalsIgnoreCase("A")) {

                                code =  lCovPrdtWriteTransformer.insertData(key, value);
                                    if (code == 0)
                                    {
                                        log.info("L_COV_PRDT_DT not written key  +++ ::time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                        L_COV_PRDT_DT_MEM_CNT_KEY prdtKey = L_COV_PRDT_DT_MEM_CNT_KEY.newBuilder()

                                                .build();
                                        return KeyValue.pair(prdtKey, null);

                                    }
                                    log.info("L_COV_PRDT_DT written key  +++ ::time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                    L_COV_PRDT_DT_MEM_CNT_KEY prdtKey = L_COV_PRDT_DT_MEM_CNT_KEY.newBuilder()
                                            .setPARTNNBR(key.getPARTNNBR())
                                            .setCNSMID(key.getCNSMID())
                                            .setCOVEFFDT(key.getCOVEFFDT())
                                            .setCOVTYPCD(key.getCOVTYPCD())
                                            .setLGCYPOLNBR(key.getLGCYPOLNBR())
                                            .setSRCCD(key.getSRCCD())
                                            .setLGCYSRCID(key.getLGCYSRCID())
                                            .build();




                                    return KeyValue.pair(prdtKey, prdtKey);
                                } else if (value != null && value.getROWSTSCD().trim().equalsIgnoreCase("D")) {



                                  code=  lCovPrdtWriteTransformer.insertData(key, value);
                                    if (code == 0)
                                    {
                                        log.info("L_COV_PRDT_DT not written key  +++ ::time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                        L_COV_PRDT_DT_MEM_CNT_KEY prdtKey = L_COV_PRDT_DT_MEM_CNT_KEY.newBuilder()

                                                .build();
                                        return KeyValue.pair(prdtKey, null);

                                    }

                                    log.info("L_COV_PRDT_DT written[Logical DELETES] key ::time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                                    L_COV_PRDT_DT_MEM_CNT_KEY prdtKey = L_COV_PRDT_DT_MEM_CNT_KEY.newBuilder()
                                            .setPARTNNBR(key.getPARTNNBR())
                                            .setCNSMID(key.getCNSMID())
                                            .setCOVEFFDT(key.getCOVEFFDT())
                                            .setCOVTYPCD(key.getCOVTYPCD())
                                            .setLGCYPOLNBR(key.getLGCYPOLNBR())
                                            .setSRCCD(key.getSRCCD())
                                            .setLGCYSRCID(key.getLGCYSRCID())
                                            .build();





                                    return KeyValue.pair(prdtKey, null);
                                } else {

                                   code =  lCovPrdtWriteTransformer.delete(key);
                                    if (code == 0)
                                    {
                                        log.info("L_COV_PRDT_DT  not written physical delete  +++ ::time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                        L_COV_PRDT_DT_MEM_CNT_KEY prdtKey = L_COV_PRDT_DT_MEM_CNT_KEY.newBuilder()

                                                .build();
                                        return KeyValue.pair(prdtKey, null);

                                    }

                                    L_COV_PRDT_DT_MEM_CNT_KEY prdtKey = L_COV_PRDT_DT_MEM_CNT_KEY.newBuilder()
                                            .setPARTNNBR(key.getPARTNNBR())
                                            .setCNSMID(key.getCNSMID())
                                            .setCOVEFFDT(key.getCOVEFFDT())
                                            .setCOVTYPCD(key.getCOVTYPCD())
                                            .setLGCYPOLNBR(key.getLGCYPOLNBR())
                                            .setSRCCD(key.getSRCCD())
                                            .setLGCYSRCID(key.getLGCYSRCID())
                                            .build();

                                    log.info("L_COV_PRDT_DT  physical delete  +++ ::time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                                    return KeyValue.pair(prdtKey, null);

                                }

                            }
                        });
                    } catch (Throwable throwable) {

                        log.error("SQLException in Table L_COV_PRDT_DT :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                        log.error("SQLException in Table L_COV_PRDT_DT :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());
                        log.error("Exception",throwable);

                        //throw new RuntimeException("");
                        log.error("Restarting L_COV_PRDT_DT POD"); System.exit(1);
                        //log.error("Exception",throwable);
                    }}
                return KeyValue.pair(key, value);


            }).to(topicConfig.getPrdtTrig());


        } catch (Exception ex) {
            log.error("Exception in Table L_COV_PRDT_DT ::" + memberKStream.transform(new MetadataTransformer()));
            ex.printStackTrace();
            log.error("Restarting L_COV_PRDT_DT POD"); System.exit(1);
        }


        return memberKStream;
    //}else return null;

        }






        @Bean
        public KStream<L_HLT_SRV_DT, com.optum.exts.cdb.model.L_HLT_SRV_DT> lHltSrvDtStream(StreamsBuilder builder) {


            if (!consumerUtil.deploymentFlag("L_HLT_SRV_DT")) {

                return null;
            }

            log.info("::::::::::::::::::::::::::::::::::::::::L_HLT_SRV_DT Consumer is Activated ::::::::::::::::::::::::::::");


            KStream<L_HLT_SRV_DT, com.optum.exts.cdb.model.L_HLT_SRV_DT>
                    memberKStream = builder.stream(topicConfig.getlHltSrvDt());

            try {
                memberKStream.map((key, value) -> {
                    final int[] retryCount = {0};
                    int code= 0 ;
                    log.info("L_HLT_SRV_DT Consuming key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                    try {
                        //  L_HLT_SRV_DT_MEM_CNT_KEY hltSrvDt = new L_HLT_SRV_DT_MEM_CNT_KEY();
                        if (value != null && value.getROWSTSCD().trim().equalsIgnoreCase("A")) {


                            code = lHtlSrvDtWriteTransformer.insertData(key, value);

                            if (code == 0)
                            {
                                log.info("L_HLT_SRV_DT not written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                L_HLT_SRV_DT_MEM_CNT_KEY hltSrvDt = L_HLT_SRV_DT_MEM_CNT_KEY.newBuilder()

                                        .build();
                                return KeyValue.pair(hltSrvDt, null);

                            }
                            log.info("L_HLT_SRV_DT written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                            L_HLT_SRV_DT_MEM_CNT_KEY hltSrvDt = L_HLT_SRV_DT_MEM_CNT_KEY.newBuilder()
                                    .setPARTNNBR(key.getPARTNNBR())
                                    .setCNSMID(key.getCNSMID())
                                    .setSRCCD(key.getSRCCD())
                                    .setLGCYPOLNBR(key.getLGCYPOLNBR())
                                    .setLGCYSRCID(key.getLGCYSRCID())
                                    .setHLTSRVPRDTLNCD(key.getHLTSRVPRDTLNCD())
                                    .setHLTSRVPRDTCD(key.getHLTSRVPRDTCD())
                                    .setHLTSRVEFFDT(key.getHLTSRVEFFDT())
                                    .build();

                            return KeyValue.pair(hltSrvDt, hltSrvDt);
                        } else if (value != null && value.getROWSTSCD().trim().equalsIgnoreCase("D")) {



                           code =  lHtlSrvDtWriteTransformer.insertData(key, value);
                            if (code == 0)
                            {
                                log.info("L_HLT_SRV_DT not written[Logical DELETES] key ::time::::"+  sdf.format(new Date()) + ":::::::" + key   );
                                L_HLT_SRV_DT_MEM_CNT_KEY hltSrvDt = L_HLT_SRV_DT_MEM_CNT_KEY.newBuilder()

                                        .build();
                                return KeyValue.pair(hltSrvDt, null);

                            }
                            log.info("L_HLT_SRV_DT written[Logical DELETES] key ::time::::"+  sdf.format(new Date()) + ":::::::" + key   );
                            //log.info("L_HLT_SRV_DT written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                            L_HLT_SRV_DT_MEM_CNT_KEY hltSrvDt = L_HLT_SRV_DT_MEM_CNT_KEY.newBuilder()
                                    .setPARTNNBR(key.getPARTNNBR())
                                    .setCNSMID(key.getCNSMID())
                                    .setSRCCD(key.getSRCCD())
                                    .setLGCYPOLNBR(key.getLGCYPOLNBR())
                                    .setLGCYSRCID(key.getLGCYSRCID())
                                    .setHLTSRVPRDTLNCD(key.getHLTSRVPRDTLNCD())
                                    .setHLTSRVPRDTCD(key.getHLTSRVPRDTCD())
                                    .setHLTSRVEFFDT(key.getHLTSRVEFFDT())
                                    .build();

                            return KeyValue.pair(hltSrvDt, null);

                        } else {


                           code = lHtlSrvDtWriteTransformer.delete(key);
                            if (code == 0)
                            {
                                log.info("L_HLT_SRV_DT not written physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                L_HLT_SRV_DT_MEM_CNT_KEY hltSrvDt = L_HLT_SRV_DT_MEM_CNT_KEY.newBuilder()

                                        .build();
                                return KeyValue.pair(hltSrvDt, null);

                            }
                            log.info("L_HLT_SRV_DT written physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                            L_HLT_SRV_DT_MEM_CNT_KEY hltSrvDt = L_HLT_SRV_DT_MEM_CNT_KEY.newBuilder()
                                    .setPARTNNBR(key.getPARTNNBR())
                                    .setCNSMID(key.getCNSMID())
                                    .setSRCCD(key.getSRCCD())
                                    .setLGCYPOLNBR(key.getLGCYPOLNBR())
                                    .setLGCYSRCID(key.getLGCYSRCID())
                                    .setHLTSRVPRDTLNCD(key.getHLTSRVPRDTLNCD())
                                    .setHLTSRVPRDTCD(key.getHLTSRVPRDTCD())
                                    .setHLTSRVEFFDT(key.getHLTSRVEFFDT())
                                    .build();



                            return KeyValue.pair(hltSrvDt, null);
                        }
                    } catch (Exception e) {
                        try {
                            retryTemplate().execute(new RetryCallback<KeyValue<L_HLT_SRV_DT_MEM_CNT_KEY, L_HLT_SRV_DT_MEM_CNT_KEY>, Exception>() {
                                @Override
                                public KeyValue<L_HLT_SRV_DT_MEM_CNT_KEY, L_HLT_SRV_DT_MEM_CNT_KEY> doWithRetry(RetryContext context) throws Exception {
                                    retryCount[0]++;
                                    int code= 0 ;
                                    log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                    //throw new SQLException("");
                                    if (value != null && value.getROWSTSCD().trim().equalsIgnoreCase("A")) {


                                       code= lHtlSrvDtWriteTransformer.insertData(key, value);

                                        if (code == 0)
                                        { log.info("L_HLT_SRV_DT not written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                            L_HLT_SRV_DT_MEM_CNT_KEY hltSrvDt = L_HLT_SRV_DT_MEM_CNT_KEY.newBuilder()

                                                    .build();
                                            return KeyValue.pair(hltSrvDt, null);

                                        }
                                        log.info("L_HLT_SRV_DT written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                        L_HLT_SRV_DT_MEM_CNT_KEY hltSrvDt = L_HLT_SRV_DT_MEM_CNT_KEY.newBuilder()
                                                .setPARTNNBR(key.getPARTNNBR())
                                                .setCNSMID(key.getCNSMID())
                                                .setSRCCD(key.getSRCCD())
                                                .setLGCYPOLNBR(key.getLGCYPOLNBR())
                                                .setLGCYSRCID(key.getLGCYSRCID())
                                                .setHLTSRVPRDTLNCD(key.getHLTSRVPRDTLNCD())
                                                .setHLTSRVPRDTCD(key.getHLTSRVPRDTCD())
                                                .setHLTSRVEFFDT(key.getHLTSRVEFFDT())
                                                .build();

                                        return KeyValue.pair(hltSrvDt, hltSrvDt);
                                    } else if (value != null && value.getROWSTSCD().trim().equalsIgnoreCase("D")) {



                                       code = lHtlSrvDtWriteTransformer.insertData(key, value);


                                        //log.info("L_HLT_SRV_DT written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                        if (code == 0)
                                        {
                                            log.info("L_HLT_SRV_DT not written[Logical DELETES] key ::time::::"+  sdf.format(new Date()) + ":::::::" + key   );
                                            L_HLT_SRV_DT_MEM_CNT_KEY hltSrvDt = L_HLT_SRV_DT_MEM_CNT_KEY.newBuilder()

                                                    .build();
                                            return KeyValue.pair(hltSrvDt, null);


                                        }
                                        log.info("L_HLT_SRV_DT written[Logical DELETES] key ::time::::"+  sdf.format(new Date()) + ":::::::" + key   );
                                        L_HLT_SRV_DT_MEM_CNT_KEY hltSrvDt = L_HLT_SRV_DT_MEM_CNT_KEY.newBuilder()
                                                .setPARTNNBR(key.getPARTNNBR())
                                                .setCNSMID(key.getCNSMID())
                                                .setSRCCD(key.getSRCCD())
                                                .setLGCYPOLNBR(key.getLGCYPOLNBR())
                                                .setLGCYSRCID(key.getLGCYSRCID())
                                                .setHLTSRVPRDTLNCD(key.getHLTSRVPRDTLNCD())
                                                .setHLTSRVPRDTCD(key.getHLTSRVPRDTCD())
                                                .setHLTSRVEFFDT(key.getHLTSRVEFFDT())
                                                .build();

                                        return KeyValue.pair(hltSrvDt, null);

                                    } else {



                                        code = lHtlSrvDtWriteTransformer.delete(key);
                                        if (code == 0)
                                        {
                                            log.info("L_HLT_SRV_DT not written physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                            L_HLT_SRV_DT_MEM_CNT_KEY hltSrvDt = L_HLT_SRV_DT_MEM_CNT_KEY.newBuilder()

                                                    .build();
                                            return KeyValue.pair(hltSrvDt, null);


                                        }
                                        log.info("L_HLT_SRV_DT written  physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                        L_HLT_SRV_DT_MEM_CNT_KEY hltSrvDt = L_HLT_SRV_DT_MEM_CNT_KEY.newBuilder()
                                                .setPARTNNBR(key.getPARTNNBR())
                                                .setCNSMID(key.getCNSMID())
                                                .setSRCCD(key.getSRCCD())
                                                .setLGCYPOLNBR(key.getLGCYPOLNBR())
                                                .setLGCYSRCID(key.getLGCYSRCID())
                                                .setHLTSRVPRDTLNCD(key.getHLTSRVPRDTLNCD())
                                                .setHLTSRVPRDTCD(key.getHLTSRVPRDTCD())
                                                .setHLTSRVEFFDT(key.getHLTSRVEFFDT())
                                                .build();



                                        return KeyValue.pair(hltSrvDt, null);
                                    }

                                }
                            });
                        } catch (Throwable throwable) {
                            log.error("Exception",throwable);
                            log.error("SQLException in Table L_HLT_SRV_DT_MEM_CNT_KEY :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                            log.error("SQLException in Table L_HLT_SRV_DT_MEM_CNT_KEY :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());


                            //throw new RuntimeException("");
                            log.error("Restarting L_HLT_SRV_DT POD"); System.exit(1);
                            //log.error("Exception",throwable);
                        }}
                    return KeyValue.pair(key, value);


                }).to(topicConfig.getHltSrvTrig());

           // });
            } catch (Exception e) {
                log.error("Exception in Table L_HLT_SRV_DT ::" + memberKStream.transform(new MetadataTransformer()));
                e.printStackTrace();
                log.error("Restarting L_HLT_SRV_DT POD"); System.exit(1);
            }


            return memberKStream;
        }


        @Bean
        public KStream<CNSM_STS, com.optum.exts.cdb.model.CNSM_STS> cnsmStsStream(StreamsBuilder builder) {

            if (!consumerUtil.deploymentFlag("CNSM_STS")) {

                return null;
            }

            log.info("::::::::::::::::::::::::::::::::::::::::CNSM_STS Consumer is Activated ::::::::::::::::::::::::::::");

            //final int[] retryCount = {0};
            KStream<CNSM_STS, com.optum.exts.cdb.model.CNSM_STS>
                    memberKStream = builder.stream(topicConfig.getCnsmSts());

            try {
                KStream stream = memberKStream.map((key, value) -> {
                    final int[] retryCount = {0};
                    log.info("CNSM_STS Consuming key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                    try {
                        if (value != null) {
                            cnsmStsWriteTransformer.insertData(key, value);
                            log.info("CNSM_STS written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                        } else {
                            cnsmStsWriteTransformer.delete(key);
                            log.info("CNSM_STS physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                        }
                    } catch (Exception e) {
                        try {
                            retryTemplate().execute(new RetryCallback<KeyValue<CNSM_STS, com.optum.exts.cdb.model.CNSM_STS>, Exception>() {
                                @Override
                                public KeyValue<CNSM_STS, com.optum.exts.cdb.model.CNSM_STS> doWithRetry(RetryContext context) throws Exception {
                                    retryCount[0]++;
                                    log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                    //throw new SQLException("");
                                    if (value != null) {
                                        cnsmStsWriteTransformer.insertData(key, value);
                                        log.info("CNSM_STS written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                    } else {
                                        cnsmStsWriteTransformer.delete(key);
                                        log.info("CNSM_STS physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                    }
                                    return KeyValue.pair(key, value);
                                }
                            });
                        } catch (Throwable throwable) {
                            log.error("Exception",throwable);
                            log.error("SQLException in Table CNSM_STS :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                            log.error("SQLException in Table CNSM_STS :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());


                            //throw new RuntimeException("");
                            log.error("Restarting CNSM_STS POD"); System.exit(1);
                            //log.error("Exception",throwable);
                        }
                    }
                    return KeyValue.pair(key, value);

                }).transform(new MetadataTransformer());
            } catch (Exception e) {
                log.error("Exception in Table CNSM_STS ::" + memberKStream.transform(new MetadataTransformer()));

                e.printStackTrace();
                log.error("Restarting CNSM_STS POD"); System.exit(1);
            }


            return memberKStream;
        }


        @Bean
        public KStream<CNSM_MDCR_ENRL, com.optum.exts.cdb.model.CNSM_MDCR_ENRL> cnsmMdcrEnrlStream(StreamsBuilder builder) {


            if (!consumerUtil.deploymentFlag("CNSM_MDCR_ENRL")) {

                return null;
            }
            log.info("::::::::::::::::::::::::::::::::::::::::CNSM_MDCR_ENRL Consumer is Activated ::::::::::::::::::::::::::::");
           // final int[] retryCount = {0};

            KStream<CNSM_MDCR_ENRL, com.optum.exts.cdb.model.CNSM_MDCR_ENRL>
                    memberKStream = builder.stream(topicConfig.getCnsmMdcrEnrl());

            try {
                KStream stream = memberKStream.map((key, value) -> {
                    final int[] retryCount = {0};
                    log.info("CNSM_MDCR_ENRL Consuming key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                    try {
                        if (value != null) {
                            cnsmMdcrEnrlWriteTransformer.insertData(key, value);
                            log.info("CNSM_MDCR_ENRL written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                        } else {
                            cnsmMdcrEnrlWriteTransformer.delete(key);
                            log.info("CNSM_MDCR_ENRL physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                        }
                    }catch (Exception e) {
                        try {
                            retryTemplate().execute(new RetryCallback<KeyValue<CNSM_MDCR_ENRL, com.optum.exts.cdb.model.CNSM_MDCR_ENRL>, Exception>() {
                                @Override
                                public KeyValue<CNSM_MDCR_ENRL, com.optum.exts.cdb.model.CNSM_MDCR_ENRL> doWithRetry(RetryContext context) throws Exception {
                                    retryCount[0]++;
                                    log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                    //throw new SQLException("");
                                    if (value != null) {
                                        cnsmMdcrEnrlWriteTransformer.insertData(key, value);
                                        log.info("CNSM_MDCR_ENRL written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                    } else {
                                        cnsmMdcrEnrlWriteTransformer.delete(key);
                                        log.info("CNSM_MDCR_ENRL physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                    }
                                    return KeyValue.pair(key, value);
                                }
                            });
                        } catch (Throwable throwable) {
                            log.error("Exception",throwable);
                            log.error("SQLException in Table CNSM_MDCR_ENRL :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                            log.error("SQLException in Table CNSM_MDCR_ENRL :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());


                            //throw new RuntimeException("");
                            log.error("Restarting CNSM_MDCR_ENRL POD"); System.exit(1);
                            //log.error("Exception",throwable);
                        }
                    }
                    return KeyValue.pair(key, value);

                }).transform(new MetadataTransformer());
            } catch (Exception e) {
                log.error("Exception in Table CNSM_MDCR_ENRL ::" + memberKStream.transform(new MetadataTransformer()));
                e.printStackTrace();
                log.error("Restarting CNSM_MDCR_ENRL POD"); System.exit(1);
            }


            return memberKStream;
        }

        @Bean
        public KStream<ML_CNSM_XREF, com.optum.exts.cdb.model.ML_CNSM_XREF> mlCnsmXrefStream(StreamsBuilder builder) {


            if (!consumerUtil.deploymentFlag("ML_CNSM_XREF")) {

                return null;
            }
            log.info("::::::::::::::::::::::::::::::::::::::::ML_CNSM_XREF Consumer is Activated ::::::::::::::::::::::::::::");
            //final int[] retryCount = {0};
            KStream<ML_CNSM_XREF, com.optum.exts.cdb.model.ML_CNSM_XREF>
                    memberKStream = builder.stream(topicConfig.getMlCnsmXref());

            try {
                KStream stream = memberKStream.map((key, value) -> {
                    log.info("ML_CNSM_XREF Consuming key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                    final int[] retryCount = {0};
                    try {
                        if (value != null) {
                            mlCnsmXrefWriteTransformer.insertData(key, value);
                            log.info("ML_CNSM_XREF written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                        } else {
                            mlCnsmXrefWriteTransformer.delete(key);
                            log.info("ML_CNSM_XREF physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                        }
                    } catch (Exception e) {
                        try {
                            retryTemplate().execute(new RetryCallback<KeyValue<ML_CNSM_XREF, com.optum.exts.cdb.model.ML_CNSM_XREF>, Exception>() {
                                @Override
                                public KeyValue<ML_CNSM_XREF, com.optum.exts.cdb.model.ML_CNSM_XREF> doWithRetry(RetryContext context) throws Exception {
                                    retryCount[0]++;
                                    log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                    //throw new SQLException("");
                                    if (value != null) {
                                        mlCnsmXrefWriteTransformer.insertData(key, value);

                                        log.info("ML_CNSM_XREF written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                    } else {
                                        mlCnsmXrefWriteTransformer.delete(key);
                                        log.info("ML_CNSM_XREF physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                    }
                                    return KeyValue.pair(key, value);
                                }
                            });
                        } catch (Throwable throwable) {
                            log.error("Exception",throwable);
                            log.error("SQLException in Table ML_CNSM_XREF :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                            log.error("SQLException in Table ML_CNSM_XREF :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());


                            //throw new RuntimeException("");
                            log.error("Restarting ML_CNSM_XREF POD"); System.exit(1);
                            //log.error("Exception",throwable);
                        }
                    }
                    return KeyValue.pair(key, value);

                }).transform(new MetadataTransformer());
            } catch (Exception e) {
                log.error("Exception in Table ML_CNSM_XREF ::" + memberKStream.transform(new MetadataTransformer()));

                e.printStackTrace();
                log.error("Restarting ML_CNSM_XREF POD"); System.exit(1);
            }


            return memberKStream;
        }


      @Bean
        public KStream<L_COV_PRDT_PCP, com.optum.exts.cdb.model.L_COV_PRDT_PCP> lCovPrdtPcpStream(StreamsBuilder builder) {
          if (!consumerUtil.deploymentFlag("L_COV_PRDT_PCP")) {

              return null;
          }

          log.info("::::::::::::::::::::::::::::::::::::::::L_COV_PRDT_PCP Consumer is Activated ::::::::::::::::::::::::::::");
          //final int[] retryCount = {0};
            KStream<L_COV_PRDT_PCP, com.optum.exts.cdb.model.L_COV_PRDT_PCP>
                    memberKStream = builder.stream(topicConfig.getlCovPrdtPcp());

            try {
                KStream stream = memberKStream.map((key, value) -> {
                    log.info("L_COV_PRDT_PCP Consuming key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                    final int[] retryCount = {0};
                    try {
                        if (value != null) {
                            lCovPrdtPcpWriteTransformer.insertData(key, value);
                            log.info("L_COV_PRDT_DT written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                        } else {
                            lCovPrdtPcpWriteTransformer.delete(key);
                            log.info("L_COV_PRDT_DT physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                        }
                    } catch (Exception e) {
                        try {
                            retryTemplate().execute(new RetryCallback<KeyValue<L_COV_PRDT_PCP, com.optum.exts.cdb.model.L_COV_PRDT_PCP>, Exception>() {
                                @Override
                                public KeyValue<L_COV_PRDT_PCP, com.optum.exts.cdb.model.L_COV_PRDT_PCP> doWithRetry(RetryContext context) throws Exception {
                                    retryCount[0]++;
                                    log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                    //throw new SQLException("");
                                    if (value != null) {
                                        lCovPrdtPcpWriteTransformer.insertData(key, value);
                                        log.info("L_COV_PRDT_PCP written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                    } else {
                                        lCovPrdtPcpWriteTransformer.delete(key);
                                        log.info("L_COV_PRDT_PCP physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                    }
                                    return KeyValue.pair(key, value);
                                }
                            });
                        } catch (Throwable throwable) {
                            log.error("Exception",throwable);
                            log.error("SQLException in Table L_COV_PRDT_PCP :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                            log.error("SQLException in Table L_COV_PRDT_PCP :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());


                            //throw new RuntimeException("");
                            log.error("Restarting L_COV_PRDT_PCP POD"); System.exit(1);
                            //log.error("Exception",throwable);
                        }
                    }


                    return KeyValue.pair(key, value);

                }).transform(new MetadataTransformer());;
            } catch (Exception e) {
                log.error("Exception in Table L_COV_PRDT_PCP ::" + memberKStream.transform(new MetadataTransformer()));
                e.printStackTrace();
                log.error("Restarting L_COV_PRDT_PCP POD"); System.exit(1);
            }


            return memberKStream;
        }


        @Bean
        public KStream<L_LF_DIS_PRDT_DT, com.optum.exts.cdb.model.L_LF_DIS_PRDT_DT> lLfDisPrdtDtStream(StreamsBuilder builder) {

            if (!consumerUtil.deploymentFlag("L_LF_DIS_PRDT_DT")) {

                return null;
            }
            log.info("::::::::::::::::::::::::::::::::::::::::L_LF_DIS_PRDT_DT Consumer is Activated ::::::::::::::::::::::::::::");
            //final int[] retryCount = {0};
            KStream<L_LF_DIS_PRDT_DT, com.optum.exts.cdb.model.L_LF_DIS_PRDT_DT>
                    memberKStream = builder.stream(topicConfig.getlLfDisPrdtDt());

            try {
                memberKStream.map((key, value) -> {
                    final int[] retryCount = {0};
                    int code= 0 ;
                    log.info("L_LF_DIS_PRDT_DT Consuming key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                    //  L_LF_DIS_PRDT_DT_MEM_CNT_KEY disPrdtDt = new L_LF_DIS_PRDT_DT_MEM_CNT_KEY();
                    try {
                        if (value != null && value.getROWSTSCD().trim().equalsIgnoreCase("A")) {
                          code=  lLfDisPrdtDtWriteTransformer.insertData(key, value);
                            if (code == 0)
                            {
                                log.info("L_LF_DIS_PRDT_DT not written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                L_LF_DIS_PRDT_DT_MEM_CNT_KEY disPrdtDt = L_LF_DIS_PRDT_DT_MEM_CNT_KEY.newBuilder()

                                        .build();
                                return KeyValue.pair(disPrdtDt, null);

                            }
                            log.info("L_LF_DIS_PRDT_DT written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                            L_LF_DIS_PRDT_DT_MEM_CNT_KEY disPrdtDt = L_LF_DIS_PRDT_DT_MEM_CNT_KEY.newBuilder()
                                    .setPARTNNBR(key.getPARTNNBR())
                                    .setCNSMID(key.getCNSMID())
                                    .setCOVEFFDT(key.getCOVEFFDT())
                                    .setCOVTYPCD(key.getCOVTYPCD())
                                    .setLGCYPOLNBR(key.getLGCYPOLNBR())
                                    .setSRCCD(key.getSRCCD())
                                    .setLGCYSRCID(key.getLGCYSRCID())
                                    .setLGCYPRDTTYPCD(key.getLGCYPRDTTYPCD())
                                    .build();

                            return KeyValue.pair(disPrdtDt, disPrdtDt);

                        }
                     else if (value != null && value.getROWSTSCD().trim().equalsIgnoreCase("D")) {




                           code =  lLfDisPrdtDtWriteTransformer.insertData(key, value);
                            if (code == 0)
                            {
                                log.info("L_LF_DIS_PRDT_DT not written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                L_LF_DIS_PRDT_DT_MEM_CNT_KEY disPrdtDt = L_LF_DIS_PRDT_DT_MEM_CNT_KEY.newBuilder()

                                        .build();
                                return KeyValue.pair(disPrdtDt, null);

                            }
                            log.info("L_LF_DIS_PRDT_DT written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                            L_LF_DIS_PRDT_DT_MEM_CNT_KEY disPrdtDt = L_LF_DIS_PRDT_DT_MEM_CNT_KEY.newBuilder()
                                    .setPARTNNBR(key.getPARTNNBR())
                                    .setCNSMID(key.getCNSMID())
                                    .setCOVEFFDT(key.getCOVEFFDT())
                                    .setCOVTYPCD(key.getCOVTYPCD())
                                    .setLGCYPOLNBR(key.getLGCYPOLNBR())
                                    .setSRCCD(key.getSRCCD())
                                    .setLGCYSRCID(key.getLGCYSRCID())
                                    .setLGCYPRDTTYPCD(key.getLGCYPRDTTYPCD())
                                    .build();

                            return KeyValue.pair(disPrdtDt, null);
                    } else {

                           code =  lLfDisPrdtDtWriteTransformer.delete(key);
                            if (code == 0)


                            {

                                log.info("L_LF_DIS_PRDT_DT not written physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                L_LF_DIS_PRDT_DT_MEM_CNT_KEY disPrdtDt = L_LF_DIS_PRDT_DT_MEM_CNT_KEY.newBuilder()

                                        .build();
                                return KeyValue.pair(disPrdtDt, null);

                            }
                            log.info("L_LF_DIS_PRDT_DT written physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                            L_LF_DIS_PRDT_DT_MEM_CNT_KEY disPrdtDt = L_LF_DIS_PRDT_DT_MEM_CNT_KEY.newBuilder()
                                    .setPARTNNBR(key.getPARTNNBR())
                                    .setCNSMID(key.getCNSMID())
                                    .setCOVEFFDT(key.getCOVEFFDT())
                                    .setCOVTYPCD(key.getCOVTYPCD())
                                    .setLGCYPOLNBR(key.getLGCYPOLNBR())
                                    .setSRCCD(key.getSRCCD())
                                    .setLGCYSRCID(key.getLGCYSRCID())
                                    .setLGCYPRDTTYPCD(key.getLGCYPRDTTYPCD())
                                    .build();

                            return KeyValue.pair(disPrdtDt, null);
                        }

                    } catch (Exception e) {
                        try {
                            retryTemplate().execute(new RetryCallback<KeyValue<L_LF_DIS_PRDT_DT_MEM_CNT_KEY, L_LF_DIS_PRDT_DT_MEM_CNT_KEY>, Exception>() {
                                @Override
                                public KeyValue<L_LF_DIS_PRDT_DT_MEM_CNT_KEY, L_LF_DIS_PRDT_DT_MEM_CNT_KEY> doWithRetry(RetryContext context) throws Exception {
                                    retryCount[0]++;
                                    int code= 0 ;
                                    log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                    //throw new SQLException("");
                                    if (value != null && value.getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                       code = lLfDisPrdtDtWriteTransformer.insertData(key, value);
                                        if (code == 0)
                                        {

                                            log.info("L_LF_DIS_PRDT_DT not written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                            L_LF_DIS_PRDT_DT_MEM_CNT_KEY disPrdtDt = L_LF_DIS_PRDT_DT_MEM_CNT_KEY.newBuilder()

                                                    .build();
                                            return KeyValue.pair(disPrdtDt, null);

                                        }
                                        log.info("L_LF_DIS_PRDT_DT written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                        L_LF_DIS_PRDT_DT_MEM_CNT_KEY disPrdtDt = L_LF_DIS_PRDT_DT_MEM_CNT_KEY.newBuilder()
                                                .setPARTNNBR(key.getPARTNNBR())
                                                .setCNSMID(key.getCNSMID())
                                                .setCOVEFFDT(key.getCOVEFFDT())
                                                .setCOVTYPCD(key.getCOVTYPCD())
                                                .setLGCYPOLNBR(key.getLGCYPOLNBR())
                                                .setSRCCD(key.getSRCCD())
                                                .setLGCYSRCID(key.getLGCYSRCID())
                                                .setLGCYPRDTTYPCD(key.getLGCYPRDTTYPCD())
                                                .build();

                                        return KeyValue.pair(disPrdtDt, disPrdtDt);

                                    }
                                    else if (value != null && value.getROWSTSCD().trim().equalsIgnoreCase("D")) {




                                       code = lLfDisPrdtDtWriteTransformer.insertData(key, value);
                                        if (code == 0)
                                        {
                                            log.info("L_LF_DIS_PRDT_DT not written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                            L_LF_DIS_PRDT_DT_MEM_CNT_KEY disPrdtDt = L_LF_DIS_PRDT_DT_MEM_CNT_KEY.newBuilder()

                                                    .build();
                                            return KeyValue.pair(disPrdtDt, null);

                                        }
                                        log.info("L_LF_DIS_PRDT_DT written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                        L_LF_DIS_PRDT_DT_MEM_CNT_KEY disPrdtDt = L_LF_DIS_PRDT_DT_MEM_CNT_KEY.newBuilder()
                                                .setPARTNNBR(key.getPARTNNBR())
                                                .setCNSMID(key.getCNSMID())
                                                .setCOVEFFDT(key.getCOVEFFDT())
                                                .setCOVTYPCD(key.getCOVTYPCD())
                                                .setLGCYPOLNBR(key.getLGCYPOLNBR())
                                                .setSRCCD(key.getSRCCD())
                                                .setLGCYSRCID(key.getLGCYSRCID())
                                                .setLGCYPRDTTYPCD(key.getLGCYPRDTTYPCD())
                                                .build();

                                        return KeyValue.pair(disPrdtDt, null);
                                    } else {

                                      code =   lLfDisPrdtDtWriteTransformer.delete(key);
                                        if (code == 0)
                                        {

                                            log.info("L_LF_DIS_PRDT_DT not written physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                            L_LF_DIS_PRDT_DT_MEM_CNT_KEY disPrdtDt = L_LF_DIS_PRDT_DT_MEM_CNT_KEY.newBuilder()

                                                    .build();
                                            return KeyValue.pair(disPrdtDt, null);

                                        }
                                        log.info("L_LF_DIS_PRDT_DT written physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                        L_LF_DIS_PRDT_DT_MEM_CNT_KEY disPrdtDt = L_LF_DIS_PRDT_DT_MEM_CNT_KEY.newBuilder()
                                                .setPARTNNBR(key.getPARTNNBR())
                                                .setCNSMID(key.getCNSMID())
                                                .setCOVEFFDT(key.getCOVEFFDT())
                                                .setCOVTYPCD(key.getCOVTYPCD())
                                                .setLGCYPOLNBR(key.getLGCYPOLNBR())
                                                .setSRCCD(key.getSRCCD())
                                                .setLGCYSRCID(key.getLGCYSRCID())
                                                .setLGCYPRDTTYPCD(key.getLGCYPRDTTYPCD())
                                                .build();

                                        return KeyValue.pair(disPrdtDt, null);
                                    }

                                }
                            });
                        } catch (Throwable throwable) {
                            log.error("Exception",throwable);
                            log.error("SQLException in Table L_LF_DIS_PRDT_DT_MEM_CNT_KEY :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                            log.error("SQLException in Table L_LF_DIS_PRDT_DT_MEM_CNT_KEY :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());


                            //throw new RuntimeException("");
                            log.error("Restarting L_LF_DIS_PRDT_DT POD"); System.exit(1);
                            //log.error("Exception",throwable);
                        }}
                    return KeyValue.pair(key, value);
                }).to(topicConfig.getDisTrig());;
            } catch (Exception e) {
                log.error("Exception in Table L_LF_DIS_PRDT_DT ::" + memberKStream.transform(new MetadataTransformer()));
                e.printStackTrace();
                log.error("Restarting L_LF_DIS_PRDT_DT POD"); System.exit(1);
            }


            return memberKStream;
        }


        @Bean
        public KStream<CNSM_DTL, com.optum.exts.cdb.model.CNSM_DTL> cnsmDtlStream(StreamsBuilder builder) {

            if (!consumerUtil.deploymentFlag("CNSM_DTL")) {

                return null;
            }
            log.info("::::::::::::::::::::::::::::::::::::::::CNSM_DTL Consumer is Activated ::::::::::::::::::::::::::::");
           // final int[] retryCount = {0};
            KStream<CNSM_DTL, com.optum.exts.cdb.model.CNSM_DTL>
                    memberKStream = builder.stream(topicConfig.getCnsmDtl());

            try {
                KStream stream = memberKStream.map((key, value) -> {
                    final int[] retryCount = {0};
                    log.info("CNSM_DTL Consuming key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                            try {
                                if (value != null) {
                                    cnsmDtlWriteTransformer.insertData(key, value);
                                    log.info("CNSM_DTL written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                } else {
                                    cnsmDtlWriteTransformer.delete(key);
                                    log.info("CNSM_DTL physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                }

                            } catch (Exception e) {
                                try {
                                    retryTemplate().execute(new RetryCallback<KeyValue<CNSM_DTL, com.optum.exts.cdb.model.CNSM_DTL>, Exception>() {
                                        @Override
                                        public KeyValue<CNSM_DTL, com.optum.exts.cdb.model.CNSM_DTL> doWithRetry(RetryContext context) throws Exception {
                                            retryCount[0]++;
                                            log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                            //throw new SQLException("");
                                            if (value != null) {
                                                cnsmDtlWriteTransformer.insertData(key, value);
                                                log.info("CNSM_DTL written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                            } else {
                                                cnsmDtlWriteTransformer.delete(key);
                                                log.info("CNSM_DTL physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                            }
                                            return KeyValue.pair(key, value);
                                        }
                                    });
                                } catch (Throwable throwable) {
                                    log.error("Exception",throwable);
                                    log.error("SQLException in Table CNSM_DTL :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                                    log.error("SQLException in Table CNSM_DTL :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());


                                    //throw new RuntimeException("");
                                    log.error("Restarting CNSM_DTL POD"); System.exit(1);
                                    //log.error("Exception",throwable);
                                }
                            }
                            return KeyValue.pair(key, value);

                        }


                ).transform(new MetadataTransformer());
            } catch (Exception e) {
                log.error("Exception in Table CNSM_DTL ::" + memberKStream.transform(new MetadataTransformer()));
                e.printStackTrace();
                log.error("Restarting CNSM_DTL POD"); System.exit(1);
            }


            return memberKStream;
        }
   @Bean
        public KStream<ML_CNSM_TEL, com.optum.exts.cdb.model.ML_CNSM_TEL> mlCnsmTelStream(StreamsBuilder builder) {

       if (!consumerUtil.deploymentFlag("ML_CNSM_TEL")) {

           return null;
       }
       log.info("::::::::::::::::::::::::::::::::::::::::ML_CNSM_TEL Consumer is Activated ::::::::::::::::::::::::::::");
       //final int[] retryCount = {0};
            KStream<ML_CNSM_TEL, com.optum.exts.cdb.model.ML_CNSM_TEL>
                    memberKStream = builder.stream(topicConfig.getMlCnsmTel());

            try {
                KStream stream = memberKStream.map((key, value) -> {
                    final int[] retryCount = {0};
                    log.info("ML_CNSM_TEL Consuming key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                    try {
                        if (value != null) {
                            // log.info("ML_CNSM_TEL Consuming key  :::"+value.toString());
                            mlCnsmTelWriteTransformer.insertData(key, value);
                            log.info("ML_CNSM_TEL written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                        } else {
                            mlCnsmTelWriteTransformer.delete(key);
                            log.info("ML_CNSM_TEL physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                        }
                    } catch (Exception e) {
                        try {
                            retryTemplate().execute(new RetryCallback<KeyValue<ML_CNSM_TEL, com.optum.exts.cdb.model.ML_CNSM_TEL>, Exception>() {
                                @Override
                                public KeyValue<ML_CNSM_TEL, com.optum.exts.cdb.model.ML_CNSM_TEL> doWithRetry(RetryContext context) throws Exception {
                                    retryCount[0]++;
                                    log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                    //throw new SQLException("");
                                    if (value != null) {
                                        mlCnsmTelWriteTransformer.insertData(key, value);
                                        log.info("ML_CNSM_TEL written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                    } else {
                                        mlCnsmTelWriteTransformer.delete(key);
                                        log.info("ML_CNSM_TEL physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                    }
                                    return KeyValue.pair(key, value);
                                }
                            });
                        } catch (Throwable throwable) {
                            log.error("Exception",throwable);
                            log.error("SQLException in Table ML_CNSM_TEL :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                            log.error("SQLException in Table ML_CNSM_TEL :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());


                            //throw new RuntimeException("");
                            log.error("Restarting ML_CNSM_TEL POD"); System.exit(1);
                            //log.error("Exception",throwable);
                        }
                    }
                    return KeyValue.pair(key, value);

                }).transform(new MetadataTransformer());
            } catch (Exception e) {
                log.error("Exception in Table ML_CNSM_TEL ::" + memberKStream.transform(new MetadataTransformer()));
                e.printStackTrace();
                log.error("Restarting ML_CNSM_TEL POD"); System.exit(1);
            }


            return memberKStream;
        }


        @Bean
        public KStream<ML_CNSM_ADR, com.optum.exts.cdb.model.ML_CNSM_ADR> mlCnsmAdrStream(StreamsBuilder builder) {

            if (!consumerUtil.deploymentFlag("ML_CNSM_ADR")) {

                return null;
            }
            log.info("::::::::::::::::::::::::::::::::::::::::ML_CNSM_ADR Consumer is Activated ::::::::::::::::::::::::::::");
            //final int[] retryCount = {0};
            KStream<ML_CNSM_ADR, com.optum.exts.cdb.model.ML_CNSM_ADR>
                    memberKStream = builder.stream(topicConfig.getMlCnsmAdr());

            try {
                KStream stream = memberKStream.map((key, value) -> {
                    final int[] retryCount = {0};
                    log.info("ML_CNSM_ADR Consuming key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                    try {

                        if (value != null) {
                            mlCnsmAdrWriteTransformer.insertData(key, value);
                            log.info("ML_CNSM_ADR written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                        } else {
                            mlCnsmAdrWriteTransformer.delete(key);
                            log.info("ML_CNSM_ADR physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                        }
                    } catch (Exception e) {
                        try {
                            retryTemplate().execute(new RetryCallback<KeyValue<ML_CNSM_ADR, com.optum.exts.cdb.model.ML_CNSM_ADR>, Exception>() {
                                @Override
                                public KeyValue<ML_CNSM_ADR, com.optum.exts.cdb.model.ML_CNSM_ADR> doWithRetry(RetryContext context) throws Exception {
                                    retryCount[0]++;
                                    log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                    //throw new SQLException("");
                                    if (value != null) {
                                        mlCnsmAdrWriteTransformer.insertData(key, value);
                                        log.info("ML_CNSM_ADR written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                    } else {
                                        mlCnsmAdrWriteTransformer.delete(key);
                                        log.info("ML_CNSM_ADR physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                    }
                                    return KeyValue.pair(key, value);
                                }
                            });
                        } catch (Throwable throwable) {
                            log.error("Exception",throwable);
                            log.error("SQLException in Table ML_CNSM_ADR :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                            log.error("SQLException in Table ML_CNSM_ADR :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());


                            //throw new RuntimeException("");
                            log.error("Restarting ML_CNSM_ADR POD"); System.exit(1);
                            //log.error("Exception",throwable);
                        }
                    }
                    return KeyValue.pair(key, value);

                }).transform(new MetadataTransformer());
            } catch (Exception e) {
                log.error("Exception in Table ML_CNSM_ADR ::" + memberKStream.transform(new MetadataTransformer()));
                e.printStackTrace();
                log.error("Restarting ML_CNSM_ADR POD"); System.exit(1);
            }


            return memberKStream;
        }


        @Bean
        public KStream<CDB_SECURITY, com.optum.exts.cdb.model.CDB_SECURITY> cdbSecurityStream(StreamsBuilder builder) {
            if (!consumerUtil.deploymentFlag("CDB_SECURITY")) {

                return null;
            }
            log.info("::::::::::::::::::::::::::::::::::::::::CDB_SECURITY Consumer is Activated ::::::::::::::::::::::::::::");
            //final int[] retryCount = {0};

            KStream<CDB_SECURITY, com.optum.exts.cdb.model.CDB_SECURITY>
                    memberKStream = builder.stream(topicConfig.getCdbSecurity());

            try {
                KStream stream = memberKStream.map((key, value) -> {
                    final int[] retryCount = {0};
                    log.info("CDB_SECURITY Consuming key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                    try {
                        if (value != null) {
                            cdbSecurityWriteTransformer.insertData(key, value);
                            log.info("CDB_SECURITY written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                        } else {
                            cdbSecurityWriteTransformer.delete(key);
                            log.info("CDB_SECURITY physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                        }

                    } catch (Exception e) {
                        try {
                            retryTemplate().execute(new RetryCallback<KeyValue<CDB_SECURITY, com.optum.exts.cdb.model.CDB_SECURITY>, Exception>() {
                                @Override
                                public KeyValue<CDB_SECURITY, com.optum.exts.cdb.model.CDB_SECURITY> doWithRetry(RetryContext context) throws Exception {
                                    retryCount[0]++;
                                    log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                    //throw new SQLException("");
                                    if (value != null) {
                                        cdbSecurityWriteTransformer.insertData(key, value);
                                        log.info("CDB_SECURITY written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                    } else {
                                        cdbSecurityWriteTransformer.delete(key);
                                        log.info("CDB_SECURITY physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                    }
                                    return KeyValue.pair(key, value);
                                }
                            });
                        } catch (Throwable throwable) {
                            log.error("Exception",throwable);
                            log.error("SQLException in Table CDB_SECURITY :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                            log.error("SQLException in Table CDB_SECURITY :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());


                            //throw new RuntimeException("");
                            log.error("Restarting COV_LVL_TYP POD"); System.exit(1);
                            //log.error("Exception",throwable);
                        }
                    }
                    return KeyValue.pair(key, value);

                }).transform(new MetadataTransformer());
            } catch (Exception e) {
                log.error("Exception in Table CDB_SECURITY ::" + memberKStream.transform(new MetadataTransformer()));

                e.printStackTrace();
                log.error("Restarting COV_LVL_TYP POD"); System.exit(1);
            }


            return memberKStream;
        }


        @Bean
        public KStream<COV_LVL_TYP, com.optum.exts.cdb.model.COV_LVL_TYP> covLvlTypStream(StreamsBuilder builder) {
            if (!consumerUtil.deploymentFlag("COV_LVL_TYP")) {

                return null;
            }
            log.info("::::::::::::::::::::::::::::::::::::::::COV_LVL_TYP Consumer is Activated ::::::::::::::::::::::::::::");
            //final int[] retryCount = {0};

            KStream<COV_LVL_TYP, com.optum.exts.cdb.model.COV_LVL_TYP>
                    memberKStream = builder.stream(topicConfig.getCovLvlTyp());

            try {
                KStream stream = memberKStream.map((key, value) -> {
                    final int[] retryCount = {0};
                    log.info("COV_LVL_TYP Consuming key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                    try {
                        if (value != null) {
                            covLvlTypeWriteTransformer.insertData(key, value);
                            log.info("COV_LVL_TYP written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                        } else {

                            covLvlTypeWriteTransformer.delete(key);
                            log.info("COV_LVL_TYP physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                        }
                    } catch (Exception e) {
                        try {
                            retryTemplate().execute(new RetryCallback<KeyValue<COV_LVL_TYP, com.optum.exts.cdb.model.COV_LVL_TYP>, Exception>() {
                                @Override
                                public KeyValue<COV_LVL_TYP, com.optum.exts.cdb.model.COV_LVL_TYP> doWithRetry(RetryContext context) throws Exception {
                                    retryCount[0]++;
                                    log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                    //throw new SQLException("");
                                    if (value != null) {
                                        covLvlTypeWriteTransformer.insertData(key, value);
                                        log.info("COV_LVL_TYP written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                    } else {
                                        covLvlTypeWriteTransformer.delete(key);
                                        log.info("COV_LVL_TYP physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                    }
                                    return KeyValue.pair(key, value);
                                }
                            });
                        } catch (Throwable throwable) {
                            log.error("Exception",throwable);

                            log.error("SQLException in Table COV_LVL_TYP :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                            log.error("SQLException in Table COV_LVL_TYP :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());


                            //throw new RuntimeException("");
                            log.error("Restarting COV_LVL_TYP POD"); System.exit(1);
                            //log.error("Exception",throwable);
                        }
                    }
                    return KeyValue.pair(key, value);

                }).transform(new MetadataTransformer());;
            } catch (Exception e) {
                log.error("Exception in Table COV_LVL_TYP ::" + memberKStream.transform(new MetadataTransformer()));
                e.printStackTrace();
                log.error("Restarting COV_LVL_TYP POD"); System.exit(1);
            }


            return memberKStream;
        }




      @Bean
        public KStream<CNSM_MDCR_PRISEC, com.optum.exts.cdb.model.CNSM_MDCR_PRISEC> cnsmMdcrPrisecStream(StreamsBuilder builder) {

          if (!consumerUtil.deploymentFlag("CNSM_MDCR_PRISEC")) {

              return null;
          }
          log.info("::::::::::::::::::::::::::::::::::::::::CNSM_MDCR_PRISEC Consumer is Activated ::::::::::::::::::::::::::::");
         // final int[] retryCount = {0};
           KStream<CNSM_MDCR_PRISEC, com.optum.exts.cdb.model.CNSM_MDCR_PRISEC>
                   memberKStream = builder.stream(topicConfig.getCnsmMdcrPrisec());

           try {
               KStream stream = memberKStream.map((key, value) -> {
                   final int[] retryCount = {0};
                   log.info("CNSM_MDCR_PRISEC Consuming key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                   try {
                       if (value != null) {
                           cnsmMdcrPrisecWriteTransformer.insertData(key, value);
                           log.info("CNSM_MDCR_PRISEC written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                       } else {
                           cnsmMdcrPrisecWriteTransformer.delete(key);
                           log.info("CNSM_MDCR_PRISEC physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                       }
                   } catch (Exception e) {
                       try {
                           retryTemplate().execute(new RetryCallback<KeyValue<CNSM_MDCR_PRISEC, com.optum.exts.cdb.model.CNSM_MDCR_PRISEC>, Exception>() {
                               @Override
                               public KeyValue<CNSM_MDCR_PRISEC, com.optum.exts.cdb.model.CNSM_MDCR_PRISEC> doWithRetry(RetryContext context) throws Exception {
                                   retryCount[0]++;
                                   log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                   //throw new SQLException("");
                                   if (value != null) {
                                       cnsmMdcrPrisecWriteTransformer.insertData(key, value);
                                       log.info("CNSM_MDCR_PRISEC written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                   } else {
                                       cnsmMdcrPrisecWriteTransformer.delete(key);
                                       log.info("CNSM_MDCR_PRISEC physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                   }
                                   return KeyValue.pair(key, value);
                               }
                           });
                       } catch (Throwable throwable) {
                           log.error("Exception",throwable);

                           log.error("SQLException in Table CNSM_MDCR_PRISEC :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                           log.error("SQLException in Table CNSM_MDCR_PRISEC :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());


                           //throw new RuntimeException("");
                           log.error("Restarting CNSM_MDCR_PRISEC POD"); System.exit(1);
                           //log.error("Exception",throwable);
                       }
                   }
                   return KeyValue.pair(key, value);

               }).transform(new MetadataTransformer());
           } catch (Exception e) {
               log.error("Exception in Table CNSM_MDCR_PRISEC ::" + memberKStream.transform(new MetadataTransformer()));
               e.printStackTrace();
               log.error("Restarting CNSM_MDCR_PRISEC POD"); System.exit(1);
           }


           return memberKStream;
       }


    @Bean
    public KStream<com.optum.exts.cdb.model.key.CNSM_AUTH_REP, com.optum.exts.cdb.model.CNSM_AUTH_REP> cnsmAuthRepStream(StreamsBuilder builder) {
        if (!consumerUtil.deploymentFlag("CNSM_AUTH_REP")) {

            return null;
        }
        log.info("::::::::::::::::::::::::::::::::::::::::CNSM_AUTH_REP Consumer is Activated ::::::::::::::::::::::::::::");
      //  final int[] retryCount = {0};
        KStream<com.optum.exts.cdb.model.key.CNSM_AUTH_REP, com.optum.exts.cdb.model.CNSM_AUTH_REP>
                memberKStream = builder.stream(topicConfig.getCnsmAuthRep());

        try {
            KStream stream = memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("CNSM_AUTH_REP Consuming key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                try {
                    if (value != null) {
                        cnsmAuthRepWriteTransformer.insertData(key, value);
                        log.info("CNSM_AUTH_REP written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                    } else {
                        cnsmAuthRepWriteTransformer.delete(key);
                        log.info("CNSM_AUTH_REP physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                    }
                }catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<CNSM_AUTH_REP, com.optum.exts.cdb.model.CNSM_AUTH_REP>, Exception>() {
                            @Override
                            public KeyValue<CNSM_AUTH_REP, com.optum.exts.cdb.model.CNSM_AUTH_REP> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (value != null) {
                                    cnsmAuthRepWriteTransformer.insertData(key, value);
                                    log.info("CNSM_AUTH_REP written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                } else {
                                    cnsmAuthRepWriteTransformer.delete(key);
                                    log.info("CNSM_AUTH_REP physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                }
                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {
                        log.error("Exception",throwable);

                        log.error("SQLException in Table CNSM_AUTH_REP :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                        log.error("SQLException in Table CNSM_AUTH_REP :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());


                        //throw new RuntimeException("");
                        log.error("Restarting CNSM_AUTH_REP POD"); System.exit(1);
                        //log.error("Exception",throwable);
                    }
                }
                return KeyValue.pair(key, value);
            }).transform(new MetadataTransformer());
        } catch (Exception e) {
            log.error("Exception in Table CNSM_AUTH_REP ::" + memberKStream.transform(new MetadataTransformer()));
            e.printStackTrace();
            log.error("Restarting CNSM_AUTH_REP POD"); System.exit(1);
        }
        return memberKStream;
    }

        @Bean
        public KStream<com.optum.exts.cdb.model.key.CNSM_CAL, com.optum.exts.cdb.model.CNSM_CAL> cnsmCalStream(StreamsBuilder builder) {
            if (!consumerUtil.deploymentFlag("CNSM_CAL")) {

                return null;
            }
            log.info("::::::::::::::::::::::::::::::::::::::::CNSM_CAL Consumer is Activated ::::::::::::::::::::::::::::");
          //  final int[] retryCount = {0};
            KStream<com.optum.exts.cdb.model.key.CNSM_CAL, com.optum.exts.cdb.model.CNSM_CAL>
                    memberKStream = builder.stream(topicConfig.getCnsmCal());

            try {
                KStream stream = memberKStream.map((key, value) -> {
                    final int[] retryCount = {0};
                    log.info("CNSM_CAL Consuming key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                    try {
                        if (value != null) {
                            cnsmCalWriteTransformer.insertData(key, value);
                            log.info("CNSM_CAL written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                        } else {
                            cnsmCalWriteTransformer.delete(key);
                            log.info("CNSM_CAL physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                        }
                    } catch (Exception e) {
                        try {
                            retryTemplate().execute(new RetryCallback<KeyValue<CNSM_CAL, com.optum.exts.cdb.model.CNSM_CAL>, Exception>() {
                                @Override
                                public KeyValue<CNSM_CAL, com.optum.exts.cdb.model.CNSM_CAL> doWithRetry(RetryContext context) throws Exception {
                                    retryCount[0]++;
                                    log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                    //throw new SQLException("");
                                    if (value != null) {
                                        cnsmCalWriteTransformer.insertData(key, value);
                                        log.info("CNSM_CAL written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                    } else {
                                        cnsmCalWriteTransformer.delete(key);
                                        log.info("CNSM_CAL physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                    }
                                    return KeyValue.pair(key, value);
                                }
                            });
                        } catch (Throwable throwable) {
                            log.error("Exception",throwable);

                            log.error("SQLException in Table CNSM_CAL :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                            log.error("SQLException in Table CNSM_CAL :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());


                            //throw new RuntimeException("");
                            log.error("Restarting CNSM_CAL POD"); System.exit(1);
                            //log.error("Exception",throwable);
                        }
                    }
                    return KeyValue.pair(key, value);
                }).transform(new MetadataTransformer());
            } catch (Exception e) {
                log.error("Exception in Table CNSM_CAL ::" + memberKStream.transform(new MetadataTransformer()));
                e.printStackTrace();
                log.error("Restarting CNSM_CAL POD"); System.exit(1);
            }
            return memberKStream;
        }


        @Bean
        public KStream<com.optum.exts.cdb.model.key.CNSM_COB_PRIMACY, com.optum.exts.cdb.model.CNSM_COB_PRIMACY> cnsmCobPrimacyStream(StreamsBuilder builder) {
            if (!consumerUtil.deploymentFlag("CNSM_COB_PRIMACY")) {

                return null;
            }
            log.info("::::::::::::::::::::::::::::::::::::::::CNSM_COB_PRIMACY Consumer is Activated ::::::::::::::::::::::::::::");
          //  final int[] retryCount = {0};
            KStream<com.optum.exts.cdb.model.key.CNSM_COB_PRIMACY, com.optum.exts.cdb.model.CNSM_COB_PRIMACY>
                    memberKStream = builder.stream(topicConfig.getCnsmCobPrimacy());

            try {
                KStream stream = memberKStream.map((key, value) -> {
                    final int[] retryCount = {0};
                    log.info("CNSM_COB_PRIMACY Consuming key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                    try {
                        if (value != null) {
                            cnsmCobPrimacyWriteTransformer.insertData(key, value);
                            log.info("CNSM_COB_PRIMACY written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                        } else {
                            cnsmCobPrimacyWriteTransformer.delete(key);
                            log.info("CNSM_COB_PRIMACY physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                        }
                    } catch (Exception e) {
                        try {
                            retryTemplate().execute(new RetryCallback<KeyValue<CNSM_COB_PRIMACY, com.optum.exts.cdb.model.CNSM_COB_PRIMACY>, Exception>() {
                                @Override
                                public KeyValue<CNSM_COB_PRIMACY, com.optum.exts.cdb.model.CNSM_COB_PRIMACY> doWithRetry(RetryContext context) throws Exception {
                                    retryCount[0]++;
                                    log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                    //throw new SQLException("");
                                    if (value != null) {
                                        cnsmCobPrimacyWriteTransformer.insertData(key, value);
                                        log.info("CNSM_COB_PRIMACY written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                    } else {
                                        cnsmCobPrimacyWriteTransformer.delete(key);
                                        log.info("CNSM_COB_PRIMACY physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                    }
                                    return KeyValue.pair(key, value);
                                }
                            });
                        } catch (Throwable throwable) {
                            log.error("Exception",throwable);

                            log.error("SQLException in Table CNSM_COB_PRIMACY :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                            log.error("SQLException in Table CNSM_COB_PRIMACY :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());


                            //throw new RuntimeException("");
                            log.error("Restarting CNSM_COB_PRIMACY POD"); System.exit(1);
                            //log.error("Exception",throwable);
                        }
                    }
                    return KeyValue.pair(key, value);
                }).transform(new MetadataTransformer());
            } catch (Exception e) {
                log.error("Exception in Table CNSM_COB_PRIMACY ::" + memberKStream.transform(new MetadataTransformer()));

                e.printStackTrace();
                log.error("Restarting CNSM_COB_PRIMACY POD"); System.exit(1);

            }
            return memberKStream;
        }

      @Bean
        public KStream<com.optum.exts.cdb.model.key.CNSM_COB_PRISEC, com.optum.exts.cdb.model.CNSM_COB_PRISEC> cnsmCobPrisecStream(StreamsBuilder builder) {
          if (!consumerUtil.deploymentFlag("CNSM_COB_PRISEC")) {

              return null;
          }
          log.info("::::::::::::::::::::::::::::::::::::::::CNSM_COB_PRISEC Consumer is Activated ::::::::::::::::::::::::::::");
          //final int[] retryCount = {0};
            KStream<com.optum.exts.cdb.model.key.CNSM_COB_PRISEC, com.optum.exts.cdb.model.CNSM_COB_PRISEC>
                    memberKStream = builder.stream(topicConfig.getCnsmCobPrisec());

            try {
                KStream stream = memberKStream.map((key, value) -> {
                    final int[] retryCount = {0};
                    log.info("CNSM_COB_PRISEC Consuming key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                    try {
                        if (value != null) {
                            cnsmCobPrisecWriteTransformer.insertData(key, value);
                            log.info("CNSM_COB_PRISEC written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                        } else {
                            cnsmCobPrisecWriteTransformer.delete(key);
                            log.info("CNSM_COB_PRISEC physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                        }
                    } catch (Exception e) {
                        try {
                            retryTemplate().execute(new RetryCallback<KeyValue<CNSM_COB_PRISEC, com.optum.exts.cdb.model.CNSM_COB_PRISEC>, Exception>() {
                                @Override
                                public KeyValue<CNSM_COB_PRISEC, com.optum.exts.cdb.model.CNSM_COB_PRISEC> doWithRetry(RetryContext context) throws Exception {
                                    retryCount[0]++;
                                    log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                    //throw new SQLException("");
                                    if (value != null) {
                                        cnsmCobPrisecWriteTransformer.insertData(key, value);
                                        log.info("CNSM_COB_PRISEC written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                    } else {
                                        cnsmCobPrisecWriteTransformer.delete(key);
                                        log.info("CNSM_COB_PRISEC physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                    }
                                    return KeyValue.pair(key, value);
                                }
                            });
                        } catch (Throwable throwable) {
                            log.error("Exception",throwable);

                            log.error("SQLException in Table CNSM_COB_PRISEC :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                            log.error("SQLException in Table CNSM_COB_PRISEC :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());


                            //throw new RuntimeException("");
                            log.error("Restarting CNSM_COB_PRISEC POD"); System.exit(1);
                            //log.error("Exception",throwable);
                        }
                    }
                    return KeyValue.pair(key, value);
                }).transform(new MetadataTransformer());
            } catch (Exception e) {
                log.error("Exception in Table CNSM_COB_PRISEC ::" + memberKStream.transform(new MetadataTransformer()));
                e.printStackTrace();
                log.error("Restarting CNSM_COB_PRISEC POD"); System.exit(1);

            }
            return memberKStream;
        }


        @Bean
        public KStream<com.optum.exts.cdb.model.key.CNSM_COV_CUST_DEFN_FLD, com.optum.exts.cdb.model.CNSM_COV_CUST_DEFN_FLD> cnsmCovCustDefnFldStream(StreamsBuilder builder) {

            if (!consumerUtil.deploymentFlag("CNSM_COV_CUST_DEFN_FLD")) {

                return null;
            }

            log.info("::::::::::::::::::::::::::::::::::::::::CNSM_COV_CUST_DEFN_FLD Consumer is Activated ::::::::::::::::::::::::::::");
            //final int[] retryCount = {0};

            KStream<com.optum.exts.cdb.model.key.CNSM_COV_CUST_DEFN_FLD, com.optum.exts.cdb.model.CNSM_COV_CUST_DEFN_FLD>
                    memberKStream = builder.stream(topicConfig.getCnsmCovCustDefnFld());

            try {
                KStream stream = memberKStream.map((key, value) -> {
                    final int[] retryCount = {0};
                    log.info("CNSM_COV_CUST_DEFN_FLD Consuming key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                    try {
                        if (value != null) {
                            cnsmCovCustDefnFldWriteTransformer.insertData(key, value);
                            log.info("CNSM_COV_CUST_DEFN_FLD written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                        } else {
                            cnsmCovCustDefnFldWriteTransformer.delete(key);
                            log.info("CNSM_COV_CUST_DEFN_FLD physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                        }
                    } catch (Exception e) {
                        try {
                            retryTemplate().execute(new RetryCallback<KeyValue<CNSM_COV_CUST_DEFN_FLD, com.optum.exts.cdb.model.CNSM_COV_CUST_DEFN_FLD>, Exception>() {
                                @Override
                                public KeyValue<CNSM_COV_CUST_DEFN_FLD, com.optum.exts.cdb.model.CNSM_COV_CUST_DEFN_FLD> doWithRetry(RetryContext context) throws Exception {
                                    retryCount[0]++;
                                    log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                    //throw new SQLException("");
                                    if (value != null) {
                                        cnsmCovCustDefnFldWriteTransformer.insertData(key, value);
                                        log.info("CNSM_COV_CUST_DEFN_FLD written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                    } else {
                                        cnsmCovCustDefnFldWriteTransformer.delete(key);
                                        log.info("CNSM_COV_CUST_DEFN_FLD physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                    }
                                    return KeyValue.pair(key, value);
                                }
                            });
                        } catch (Throwable throwable) {
                            log.error("Exception",throwable);

                            log.error("SQLException in Table CNSM_COV_CUST_DEFN_FLD :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                            log.error("SQLException in Table CNSM_COV_CUST_DEFN_FLD :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());


                            //throw new RuntimeException("");
                            log.error("Restarting CNSM_COV_CUST_DEFN_FLD POD"); System.exit(1);
                            //log.error("Exception",throwable);
                        }
                    }
                    return KeyValue.pair(key, value);
                }).transform(new MetadataTransformer());
            } catch (Exception e) {
                log.error("Exception in Table CNSM_COV_CUST_DEFN_FLD ::" + memberKStream.transform(new MetadataTransformer()));
                e.printStackTrace();
                log.error("Restarting CNSM_COV_CUST_DEFN_FLD POD"); System.exit(1);
            }
            return memberKStream;
        }

    @Bean
    public KStream<CNSM_MDCR_ENTL, com.optum.exts.cdb.model.CNSM_MDCR_ENTL> CnsmMdcrEntlStream(StreamsBuilder builder) {
        if (!consumerUtil.deploymentFlag("CNSM_MDCR_ENTL")) {

            return null;
        }
        log.info("::::::::::::::::::::::::::::::::::::::::CNSM_MDCR_ENTL Consumer is Activated ::::::::::::::::::::::::::::");
       // final int[] retryCount = {0};
        KStream<CNSM_MDCR_ENTL, com.optum.exts.cdb.model.CNSM_MDCR_ENTL>
                memberKStream = builder.stream(topicConfig.getCnsmMdcrEntl());

        try {
            KStream stream = memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                //log.info("CNSM_MDCR_ENTL Consuming key  :::"+key.toString());
                log.info("CNSM_MDCR_ENTL Consuming key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                try {
                    if (value != null) {

                        // log.info("L_COV_PRDT_DT Consuming key  :::"+value.toString());

                        cnsmMdcrEntlWriteTransformer.insertData(key, value);
                        log.info("CNSM_MDCR_ENTL written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());


                    } else {

                        cnsmMdcrEntlWriteTransformer.delete(key);
                        log.info("CNSM_MDCR_ENTL physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());


                    }
                } catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<CNSM_MDCR_ENTL, com.optum.exts.cdb.model.CNSM_MDCR_ENTL>, Exception>() {
                            @Override
                            public KeyValue<CNSM_MDCR_ENTL, com.optum.exts.cdb.model.CNSM_MDCR_ENTL> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (value != null) {
                                    cnsmMdcrEntlWriteTransformer.insertData(key, value);
                                    log.info("CNSM_MDCR_ENTL written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                } else {
                                    cnsmMdcrEntlWriteTransformer.delete(key);
                                    log.info("CNSM_MDCR_ENTL physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                }
                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {
                        log.error("Exception",throwable);

                        log.error("SQLException in Table CNSM_MDCR_ENTL :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                        log.error("SQLException in Table CNSM_MDCR_ENTL :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());


                        //throw new RuntimeException("");
                        log.error("Restarting CNSM_MDCR_ENTL POD"); System.exit(1);
                        //log.error("Exception",throwable);
                    }
                }
                return KeyValue.pair(key, value);

            }).transform(new MetadataTransformer());
        } catch (Exception e) {
            log.error("Exception in Table CNSM_MDCR_ENTL ::" + memberKStream.transform(new MetadataTransformer()));
            e.printStackTrace();
            log.error("Restarting CNSM_MDCR_ENTL POD"); System.exit(1);
        }


        return memberKStream;
    }

    @Bean
    public KStream<CNSM_OTHR_INS, com.optum.exts.cdb.model.CNSM_OTHR_INS> CnsmOthrInsStream(StreamsBuilder builder) {
        if (!consumerUtil.deploymentFlag("CNSM_OTHR_INS")) {

            return null;
        }
        log.info("::::::::::::::::::::::::::::::::::::::::CNSM_OTHR_INS Consumer is Activated ::::::::::::::::::::::::::::");

        KStream<CNSM_OTHR_INS, com.optum.exts.cdb.model.CNSM_OTHR_INS>
                memberKStream = builder.stream(topicConfig.getCnsmOthrIns());

        try {
            KStream stream = memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("CNSM_OTHR_INS Consuming key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                try {   //partn_nbr,cnsm_id,src_cd,lgcy_src_id,cob_cov_typ_cd,oi_eff_dt
                    if (value != null) {
                        cnsmOthrInsWriteTransformer.insertData(key, value);
                        log.info("CNSM_OTHR_INS written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());



                        //return KeyValue.pair(prdtKey, prdtKey);
                    } else {

                        cnsmOthrInsWriteTransformer.delete(key);
                        log.info("CNSM_OTHR_INS physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                        //return KeyValue.pair(prdtKey, null);

                    }
                } catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<CNSM_OTHR_INS, com.optum.exts.cdb.model.CNSM_OTHR_INS>, Exception>() {
                            @Override
                            public KeyValue<CNSM_OTHR_INS, com.optum.exts.cdb.model.CNSM_OTHR_INS> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (value != null) {
                                    cnsmOthrInsWriteTransformer.insertData(key, value);
                                    log.info("CNSM_OTHR_INS written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                } else {
                                    cnsmOthrInsWriteTransformer.delete(key);
                                    log.info("CNSM_OTHR_INS physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                }
                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {
                        log.error("Exception",throwable);

                        log.error("SQLException in Table CNSM_OTHR_INS :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                        log.error("SQLException in Table CNSM_OTHR_INS :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());


                        //throw new RuntimeException("");
                        log.error("Restarting CNSM_OTHR_INS POD"); System.exit(1);
                        //log.error("Exception",throwable);
                    }
                }

                return KeyValue.pair(key, value);
            }).transform(new MetadataTransformer());
        } catch (Exception e) {
            log.error("Exception in Table CNSM_OTHR_INS ::" + memberKStream.transform(new MetadataTransformer()));
            e.printStackTrace();
            log.error("Restarting CNSM_OTHR_INS POD"); System.exit(1);
        }


        return memberKStream;
    }






    @Bean
    public KStream<ML_CNSM_ELCTR_ADR, com.optum.exts.cdb.model.ML_CNSM_ELCTR_ADR> mlCnsmElctrAdrStream(StreamsBuilder builder) {

        if (!consumerUtil.deploymentFlag("ML_CNSM_ELCTR_ADR")) {

            return null;
        }
        log.info("::::::::::::::::::::::::::::::::::::::::ML_CNSM_ELCTR_ADR Consumer is Activated ::::::::::::::::::::::::::::");

        KStream<ML_CNSM_ELCTR_ADR, com.optum.exts.cdb.model.ML_CNSM_ELCTR_ADR>
                memberKStream = builder.stream(topicConfig.getMlCnsmElctrAdr());

        try {
            KStream stream = memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("ML_CNSM_ELCTR_ADR Consuming key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                try {

                    if (value != null) {
                        mlCnsmElctrAdrWriteTransformer.insertData(key, value);
                        log.info("ML_CNSM_ELCTR_ADR written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                    } else {


                        mlCnsmElctrAdrWriteTransformer.delete(key);
                        log.info("ML_CNSM_ELCTR_ADR physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                    }

                } catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<ML_CNSM_ELCTR_ADR, com.optum.exts.cdb.model.ML_CNSM_ELCTR_ADR>, Exception>() {
                            @Override
                            public KeyValue<ML_CNSM_ELCTR_ADR, com.optum.exts.cdb.model.ML_CNSM_ELCTR_ADR> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (value != null) {
                                    mlCnsmElctrAdrWriteTransformer.insertData(key, value);
                                    log.info("ML_CNSM_ELCTR_ADR written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                } else {
                                    mlCnsmElctrAdrWriteTransformer.delete(key);
                                    log.info("ML_CNSM_ELCTR_ADR physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                }
                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {
                        log.error("Exception",throwable);

                        log.error("SQLException in Table ML_CNSM_ELCTR_ADR :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                        log.error("SQLException in Table ML_CNSM_ELCTR_ADR :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());


                        //throw new RuntimeException("");
                        log.error("Restarting ML_CNSM_ELCTR_ADR POD"); System.exit(1);
                        //log.error("Exception",throwable);
                    }
                }
                return KeyValue.pair(key, value);

            }).transform(new MetadataTransformer());
        } catch (Exception e) {
            log.error("Exception in Table ML_CNSM_ELCTR_ADR ::" + memberKStream.transform(new MetadataTransformer()));
            e.printStackTrace();
            log.error("Restarting ML_CNSM_ELCTR_ADR POD"); System.exit(1);
        }


        return memberKStream;
    }
    @Bean
    public KStream<com.optum.exts.cdb.model.key.CNSM_CUST_DEFN_FLD, com.optum.exts.cdb.model.CNSM_CUST_DEFN_FLD> CNSM_CUST_DEFN_FLD(StreamsBuilder builder) {
        if (!consumerUtil.deploymentFlag("CNSM_CUST_DEFN_FLD")) {

            return null;
        }
        log.info("::::::::::::::::::::::::::::::::::::::::CNSM_CUST_DEFN_FLD Consumer is Activated ::::::::::::::::::::::::::::");
       // final int[] retryCount = {0};
        KStream<com.optum.exts.cdb.model.key.CNSM_CUST_DEFN_FLD, com.optum.exts.cdb.model.CNSM_CUST_DEFN_FLD>
                memberKStream = builder.stream(topicConfig.getcnsmCustDefnFld());

        try {
            KStream stream = memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("CNSM_CUST_DEFN_FLD Consuming key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                try {
                    if (value != null) {
                        cnsmCustDefnFldWriteTransformer.insertData(key, value);
                        log.info("CNSM_CUST_DEFN_FLD written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                    } else {
                        cnsmCustDefnFldWriteTransformer.delete(key);
                        log.info("CNSM_CUST_DEFN_FLD physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                    }
                } catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<CNSM_CUST_DEFN_FLD, com.optum.exts.cdb.model.CNSM_CUST_DEFN_FLD>, Exception>() {
                            @Override
                            public KeyValue<CNSM_CUST_DEFN_FLD, com.optum.exts.cdb.model.CNSM_CUST_DEFN_FLD> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (value != null) {
                                    cnsmCustDefnFldWriteTransformer.insertData(key, value);
                                    log.info("CNSM_CUST_DEFN_FLD written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                } else {
                                    cnsmCustDefnFldWriteTransformer.delete(key);
                                    log.info("CNSM_CUST_DEFN_FLD physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                }
                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {
                        log.error("Exception",throwable);

                        log.error("SQLException in Table CNSM_CUST_DEFN_FLD :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                        log.error("SQLException in Table CNSM_CUST_DEFN_FLD :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());


                        //throw new RuntimeException("");
                        log.error("Restarting CNSM_CUST_DEFN_FLD POD"); System.exit(1);
                        //log.error("Exception",throwable);
                    }
                }
                return KeyValue.pair(key, value);
            }).transform(new MetadataTransformer());
        } catch (Exception e) {
            log.error("Exception in Table CNSM_CUST_DEFN_FLD ::" + memberKStream.transform(new MetadataTransformer()));
            e.printStackTrace();
            log.error("Restarting CNSM_CUST_DEFN_FLD POD"); System.exit(1);
        }
        return memberKStream;
    }

    @Bean
    public KStream<com.optum.exts.cdb.model.key.CNSM_EFT, com.optum.exts.cdb.model.CNSM_EFT> CNSM_EFT(StreamsBuilder builder) {
        if (!consumerUtil.deploymentFlag("CNSM_EFT")) {

            return null;
        }
        log.info("::::::::::::::::::::::::::::::::::::::::CNSM_EFT Consumer is Activated ::::::::::::::::::::::::::::");
        //final int[] retryCount = {0};
        KStream<com.optum.exts.cdb.model.key.CNSM_EFT, com.optum.exts.cdb.model.CNSM_EFT>
                memberKStream = builder.stream(topicConfig.getCnsmEft());

        try {
            KStream stream = memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("CNSM_EFT Consuming key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                try {
                    if (value != null) {
                        cnsmEftWriteTransformer.insertData(key, value);
                        log.info("CNSM_EFT written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                    } else {
                        cnsmEftWriteTransformer.delete(key);
                        log.info("CNSM_EFT physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                    }
                } catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<CNSM_EFT, com.optum.exts.cdb.model.CNSM_EFT>, Exception>() {
                            @Override
                            public KeyValue<CNSM_EFT, com.optum.exts.cdb.model.CNSM_EFT> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (value != null) {
                                    cnsmEftWriteTransformer.insertData(key, value);
                                    log.info("CNSM_EFT written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                } else {
                                    cnsmEftWriteTransformer.delete(key);
                                    log.info("CNSM_EFT physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                }
                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {
                        log.error("Exception",throwable);

                        log.error("SQLException in Table CNSM_EFT :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                        log.error("SQLException in Table CNSM_EFT :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());


                        //throw new RuntimeException("");
                        log.error("Restarting CNSM_EFT POD"); System.exit(1);
                        //log.error("Exception",throwable);
                    }
                }
                return KeyValue.pair(key, value);
            }).transform(new MetadataTransformer());;
        } catch (Exception e) {
            log.error("Exception in Table CNSM_EFT ::" + memberKStream.transform(new MetadataTransformer()));
            e.printStackTrace();
            log.error("Restarting CNSM_EFT POD"); System.exit(1);
        }
        return memberKStream;
    }

    @Bean
    public KStream<com.optum.exts.cdb.model.key.CNSM_MDCR_ELIG, com.optum.exts.cdb.model.CNSM_MDCR_ELIG> CNSM_MDCR_ELIG(StreamsBuilder builder) {
        if (!consumerUtil.deploymentFlag("CNSM_MDCR_ELIG")) {

            return null;
        }
        log.info("::::::::::::::::::::::::::::::::::::::::CNSM_MDCR_ELIG Consumer is Activated ::::::::::::::::::::::::::::");
        //final int[] retryCount = {0};
        KStream<com.optum.exts.cdb.model.key.CNSM_MDCR_ELIG, com.optum.exts.cdb.model.CNSM_MDCR_ELIG>
                memberKStream = builder.stream(topicConfig.getcnsmMdcrElig());

        try {
            KStream stream = memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("CNSM_MDCR_ELIG Consuming key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                try {
                    if (value != null) {
                        cnmsMdcrEligWriteTransformer.insertData(key, value);
                        log.info("CNSM_MDCR_ELIG written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                    } else {
                        cnmsMdcrEligWriteTransformer.delete(key);
                        log.info("CNSM_MDCR_ELIG physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                    }
                }catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<CNSM_MDCR_ELIG, com.optum.exts.cdb.model.CNSM_MDCR_ELIG>, Exception>() {
                            @Override
                            public KeyValue<CNSM_MDCR_ELIG, com.optum.exts.cdb.model.CNSM_MDCR_ELIG> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (value != null) {
                                    cnmsMdcrEligWriteTransformer.insertData(key, value);
                                    log.info("CNSM_MDCR_ELIG written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                } else {
                                    cnmsMdcrEligWriteTransformer.delete(key);
                                    log.info("CNSM_MDCR_ELIG physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                }
                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {
                        log.error("Exception",throwable);

                        log.error("SQLException in Table CNSM_MDCR_ELIG :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                        log.error("SQLException in Table CNSM_MDCR_ELIG :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());


                        //throw new RuntimeException("");
                        log.error("Restarting CNSM_MDCR_ELIG POD"); System.exit(1);
                        //log.error("Exception",throwable);
                    }
                }
                return KeyValue.pair(key, value);
            }).transform(new MetadataTransformer());;
        } catch (Exception e) {
            log.error("Exception in Table CNSM_MDCR_ELIG ::" + memberKStream.transform(new MetadataTransformer()));
            e.printStackTrace();
            log.error("Restarting CNSM_MDCR_ELIG POD"); System.exit(1);
        }
        return memberKStream;
    }

    @Bean
    public KStream<com.optum.exts.cdb.model.key.CNSM_MDCR_PRIMACY, com.optum.exts.cdb.model.CNSM_MDCR_PRIMACY> CNSM_MDCR_PRIMACY(StreamsBuilder builder) {
        if (!consumerUtil.deploymentFlag("CNSM_MDCR_PRIMACY")) {

            return null;
        }
        log.info("::::::::::::::::::::::::::::::::::::::::CNSM_MDCR_PRIMACY Consumer is Activated ::::::::::::::::::::::::::::");
        //final int[] retryCount = {0};
        KStream<com.optum.exts.cdb.model.key.CNSM_MDCR_PRIMACY, com.optum.exts.cdb.model.CNSM_MDCR_PRIMACY>
                memberKStream = builder.stream(topicConfig.getcnsmMdcrPrimacy());

        try {
            KStream stream = memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("CNSM_MDCR_PRIMACY Consuming key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                try {
                    if (value != null) {
                        cnmsMdcrPrimacyWriteTransformer.insertData(key, value);
                        log.info("CNSM_MDCR_PRIMACY written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                    } else {
                        cnmsMdcrPrimacyWriteTransformer.delete(key);
                        log.info("CNSM_MDCR_PRIMACY physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                    }
                } catch (Exception e) {

                    try {
                        e.printStackTrace();


                        retryTemplate().execute(new RetryCallback<KeyValue<CNSM_MDCR_PRIMACY, com.optum.exts.cdb.model.CNSM_MDCR_PRIMACY>, Exception>() {
                            @Override
                            public KeyValue<CNSM_MDCR_PRIMACY, com.optum.exts.cdb.model.CNSM_MDCR_PRIMACY> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (value != null) {
                                    cnmsMdcrPrimacyWriteTransformer.insertData(key, value);
                                    log.info("CNSM_MDCR_PRIMACY written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                } else {
                                    cnmsMdcrPrimacyWriteTransformer.delete(key);
                                    log.info("CNSM_MDCR_PRIMACY physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                }
                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {
                       // log.error(log.error("Exception",throwable));



                        log.error("SQLException in Table CNSM_MDCR_PRIMACY :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                        log.error("SQLException in Table CNSM_MDCR_PRIMACY :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());

                        log.error("Exception",throwable);
                        //throw new RuntimeException("");
                        log.error("Restarting CNSM_MDCR_PRIMACY POD"); System.exit(1);
                        //log.error("Exception",throwable);
                    }
                }
                return KeyValue.pair(key, value);
            }).transform(new MetadataTransformer());;
        } catch (Exception e) {
            log.error("Exception in Table CNSM_MDCR_PRIMACY ::" + memberKStream.transform(new MetadataTransformer()));
            e.printStackTrace();
            log.error("Restarting CNSM_MDCR_PRIMACY POD"); System.exit(1);
        }
        return memberKStream;
    }

    @Bean
    public KStream<com.optum.exts.cdb.model.key.CNSM_PRXST_COND, com.optum.exts.cdb.model.CNSM_PRXST_COND> CNSM_PRXST_COND(StreamsBuilder builder) {
        if (!consumerUtil.deploymentFlag("CNSM_PRXST_COND")) {

            return null;
        }
        log.info("::::::::::::::::::::::::::::::::::::::::CNSM_PRXST_COND Consumer is Activated ::::::::::::::::::::::::::::");
        //final int[] retryCount = {0};
        KStream<com.optum.exts.cdb.model.key.CNSM_PRXST_COND, com.optum.exts.cdb.model.CNSM_PRXST_COND>
                memberKStream = builder.stream(topicConfig.getcnsmPrxstCond());

        try {
            KStream stream = memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("CNSM_PRXST_COND Consuming key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                try {
                    if (value != null) {
                        cnmsPrxstCondWriteTransformer.insertData(key, value);
                        log.info("CNSM_PRXST_COND written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                    } else {
                        cnmsPrxstCondWriteTransformer.delete(key);
                        log.info("CNSM_PRXST_COND physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                    }
                } catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<CNSM_PRXST_COND, com.optum.exts.cdb.model.CNSM_PRXST_COND>, Exception>() {
                            @Override
                            public KeyValue<CNSM_PRXST_COND, com.optum.exts.cdb.model.CNSM_PRXST_COND> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (value != null) {
                                    cnmsPrxstCondWriteTransformer.insertData(key, value);
                                    log.info("CNSM_PRXST_COND written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                } else {
                                    cnmsPrxstCondWriteTransformer.delete(key);
                                    log.info("CNSM_PRXST_COND physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                }
                                return KeyValue.pair(key, value);
                            }
                        }


                        );
                    } catch (Throwable throwable) {
                        log.error("Exception",throwable);

                        log.error("SQLException in Table CNSM_PRXST_COND :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                        log.error("SQLException in Table CNSM_PRXST_COND :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());


                        //throw new RuntimeException("");
                        log.error("Restarting CNSM_PRXST_COND POD"); System.exit(1);
                        //log.error("Exception",throwable);
                    }
                }
                return KeyValue.pair(key, value);
            }).transform(new MetadataTransformer());;
        } catch (Exception e) {
            log.error("Exception in Table CNSM_PRXST_COND ::" + memberKStream.transform(new MetadataTransformer()));
            e.printStackTrace();
            log.error("Restarting CNSM_PRXST_COND POD"); System.exit(1);
        }
        return memberKStream;
    }

    @Bean
    public KStream<com.optum.exts.cdb.model.key.CNSM_SLRY_BAS_DED_OOP, com.optum.exts.cdb.model.CNSM_SLRY_BAS_DED_OOP> CNSM_SLRY_BAS_DED_OOP(StreamsBuilder builder) {
        if (!consumerUtil.deploymentFlag("CNSM_SLRY_BAS_DED_OOP")) {

            return null;
        }
        log.info("::::::::::::::::::::::::::::::::::::::::CNSM_SLRY_BAS_DED_OOP Consumer is Activated ::::::::::::::::::::::::::::");
       // final int[] retryCount = {0};
        KStream<com.optum.exts.cdb.model.key.CNSM_SLRY_BAS_DED_OOP, com.optum.exts.cdb.model.CNSM_SLRY_BAS_DED_OOP>
                memberKStream = builder.stream(topicConfig.getCnsmSlryBasDedOop());

        try {
            KStream stream = memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("CNSM_SLRY_BAS_DED_OOP Consuming key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                try {
                    if (value != null) {
                        cnsmSlryBasDedOopWriteTransformer.insertData(key, value);
                        log.info("CNSM_SLRY_BAS_DED_OOP written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                    } else {
                        cnsmSlryBasDedOopWriteTransformer.delete(key);
                        log.info("CNSM_SLRY_BAS_DED_OOP physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                    }
                } catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<CNSM_SLRY_BAS_DED_OOP, com.optum.exts.cdb.model.CNSM_SLRY_BAS_DED_OOP>, Exception>() {
                            @Override
                            public KeyValue<CNSM_SLRY_BAS_DED_OOP, com.optum.exts.cdb.model.CNSM_SLRY_BAS_DED_OOP> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (value != null) {
                                    cnsmSlryBasDedOopWriteTransformer.insertData(key, value);
                                    log.info("CNSM_SLRY_BAS_DED_OOP written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                } else {
                                    cnsmSlryBasDedOopWriteTransformer.delete(key);
                                    log.info("CNSM_SLRY_BAS_DED_OOP physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                }
                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {
                        log.error("Exception",throwable);

                        log.error("SQLException in Table CNSM_SLRY_BAS_DED_OOP :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                        log.error("SQLException in Table CNSM_SLRY_BAS_DED_OOP :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());


                        //throw new RuntimeException("");
                        log.error("Restarting CNSM_SLRY_BAS_DED_OOP POD"); System.exit(1);
                        //log.error("Exception",throwable);
                    }
                }
                return KeyValue.pair(key, value);
            }).transform(new MetadataTransformer());;
        } catch (Exception e) {
            log.error("Exception in Table CNSM_SLRY_BAS_DED_OOP ::" + memberKStream.transform(new MetadataTransformer()));
            e.printStackTrace();
            log.error("Restarting CNSM_SLRY_BAS_DED_OOP POD"); System.exit(1);
        }
        return memberKStream;
    }



    @Bean
    public KStream<COV_INFO, com.optum.exts.cdb.model.COV_INFO> covInfoStream(StreamsBuilder builder) {

        if (!consumerUtil.deploymentFlag("COV_INFO")) {

            return null;
        }
        log.info("::::::::::::::::::::::::::::::::::::::::COV_INFO Consumer is Activated ::::::::::::::::::::::::::::");
        //final int[] retryCount = {0};
//log.info("+++++++++++++++++++++++"+topicConfig.getAutoOffsetReset());

        KStream<COV_INFO, com.optum.exts.cdb.model.COV_INFO>
                memberKStream = builder.stream(topicConfig.getCovInfo());

        try {
            KStream stream = memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("COV_INFO Consuming key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                try {
                    if (value != null) {
                        covInfoWriteTransformerWriteTransformer.insertData(key, value);
                        log.info("COV_INFO written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                    } else {

                        covInfoWriteTransformerWriteTransformer.delete(key);
                        log.info("COV_INFO physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                    }
                } catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<COV_INFO, com.optum.exts.cdb.model.COV_INFO>, Exception>() {
                            @Override
                            public KeyValue<COV_INFO, com.optum.exts.cdb.model.COV_INFO> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (value != null) {
                                    covInfoWriteTransformerWriteTransformer.insertData(key, value);
                                    log.info("COV_INFO written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                } else {
                                    covInfoWriteTransformerWriteTransformer.delete(key);
                                    log.info("COV_INFO physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                }
                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {
                        log.error("Exception",throwable);

                        log.error("SQLException in Table COV_INFO :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                        log.error("SQLException in Table COV_INFO :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());


                        //throw new RuntimeException("");
                        log.error("Restarting COV_INFO POD"); System.exit(1);
                        //log.error("Exception",throwable);
                    }
                }
                return KeyValue.pair(key, value);

            }).transform(new MetadataTransformer());
        } catch (Exception e) {
            log.error("Exception in Table COV_INFO ::" + memberKStream.transform(new MetadataTransformer()));
            e.printStackTrace();
            log.error("Restarting COV_INFO POD"); System.exit(1);
        }


        return memberKStream;
    }





    @Bean
    public KStream<CUST_INFO, com.optum.exts.cdb.model.CUST_INFO> custInfoStream(StreamsBuilder builder) {

        if (!consumerUtil.deploymentFlag("CUST_INFO")) {

            return null;
        }
        log.info("::::::::::::::::::::::::::::::::::::::::CUST_INFO Consumer is Activated ::::::::::::::::::::::::::::");
      //  final int[] retryCount = {0};
//log.info("+++++++++++++++++++++++"+topicConfig.getAutoOffsetReset());

        KStream<CUST_INFO, com.optum.exts.cdb.model.CUST_INFO>
                memberKStream = builder.stream(topicConfig.getCustInfo());

        try {
            KStream stream = memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("CUST_INFO Consuming key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                try {
                    if (value != null) {
                        custInfoWriteTransformer.insertData(key, value);
                        log.info("CUST_INFO written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                    } else {

                        custInfoWriteTransformer.delete(key);
                        log.info("CUST_INFO physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                    }
                } catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<CUST_INFO, com.optum.exts.cdb.model.CUST_INFO>, Exception>() {
                            @Override
                            public KeyValue<CUST_INFO, com.optum.exts.cdb.model.CUST_INFO> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (value != null) {
                                    custInfoWriteTransformer.insertData(key, value);
                                    log.info("CUST_INFO written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                } else {
                                    custInfoWriteTransformer.delete(key);
                                    log.info("CUST_INFO physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                }
                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {
                        log.error("Exception",throwable);

                        log.error("SQLException in Table CUST_INFO :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                        log.error("SQLException in Table CUST_INFO :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());


                        //throw new RuntimeException("");
                        log.error("Restarting CUST_INFO POD"); System.exit(1);
                        //log.error("Exception",throwable);
                    }
                }
                return KeyValue.pair(key, value);

            }).transform(new MetadataTransformer());
        } catch (Exception e) {
            log.error("Exception in Table CUST_INFO ::" + memberKStream.transform(new MetadataTransformer()));
            e.printStackTrace();
            log.error("Restarting CUST_INFO POD"); System.exit(1);
        }


        return memberKStream;
    }



    @Bean
    public KStream<PLN_BEN_SET, com.optum.exts.cdb.model.PLN_BEN_SET> plnBenSetStream(StreamsBuilder builder) {

        if (!consumerUtil.deploymentFlag("PLN_BEN_SET")) {

            return null;
        }
        log.info("::::::::::::::::::::::::::::::::::::::::PLN_BEN_SET Consumer is Activated ::::::::::::::::::::::::::::");
      //  final int[] retryCount = {0};
//log.info("+++++++++++++++++++++++"+topicConfig.getAutoOffsetReset());

        KStream<PLN_BEN_SET, com.optum.exts.cdb.model.PLN_BEN_SET>
                memberKStream = builder.stream(topicConfig.getPlnBenSet());

        try {
            KStream stream = memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("PLN_BEN_SET Consuming key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                try {
                    if (value != null) {
                        plnBenSetWriteTransformer.insertData(key, value);
                        log.info("PLN_BEN_SET written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                    } else {

                        plnBenSetWriteTransformer.delete(key);
                        log.info("PLN_BEN_SET physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                    }
                } catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<PLN_BEN_SET, com.optum.exts.cdb.model.PLN_BEN_SET>, Exception>() {
                            @Override
                            public KeyValue<PLN_BEN_SET, com.optum.exts.cdb.model.PLN_BEN_SET> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (value != null) {
                                    plnBenSetWriteTransformer.insertData(key, value);
                                    log.info("PLN_BEN_SET written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                } else {
                                    plnBenSetWriteTransformer.delete(key);
                                    log.info("PLN_BEN_SET physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                }
                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {
                        log.error("Exception",throwable);

                        log.error("SQLException in Table PLN_BEN_SET :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                        log.error("SQLException in Table PLN_BEN_SET :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());


                        //throw new RuntimeException("");
                        log.error("Restarting PLN_BEN_SET POD"); System.exit(1);
                        //log.error("Exception",throwable);
                    }
                }
                return KeyValue.pair(key, value);

            }).transform(new MetadataTransformer());
        } catch (Exception e) {
            log.error("Exception in Table PLN_BEN_SET ::" + memberKStream.transform(new MetadataTransformer()));
            e.printStackTrace();
            log.error("Restarting PLN_BEN_SET POD"); System.exit(1);
        }


        return memberKStream;
    }



    @Bean
    public KStream<PLN_BEN_SET_DET, com.optum.exts.cdb.model.PLN_BEN_SET_DET> plnBenSetDetStream(StreamsBuilder builder) {
        if (!consumerUtil.deploymentFlag("PLN_BEN_SET_DET")) {

            return null;
        }
        log.info("::::::::::::::::::::::::::::::::::::::::PLN_BEN_SET_DET Consumer is Activated ::::::::::::::::::::::::::::");
        //final int[] retryCount = {0};

//log.info("+++++++++++++++++++++++"+topicConfig.getAutoOffsetReset());

        KStream<PLN_BEN_SET_DET, com.optum.exts.cdb.model.PLN_BEN_SET_DET>
                memberKStream = builder.stream(topicConfig.getPlnBenSetDet());

        try {
            KStream stream = memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("PLN_BEN_SET_DET Consuming key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                try {
                    if (value != null) {
                        plnBenSetDetWriteTransformer.insertData(key, value);
                        log.info("PLN_BEN_SET_DET written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                    } else {

                        plnBenSetDetWriteTransformer.delete(key);
                        log.info("PLN_BEN_SET_DET physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                    }
                } catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<PLN_BEN_SET_DET, com.optum.exts.cdb.model.PLN_BEN_SET_DET>, Exception>() {
                            @Override
                            public KeyValue<PLN_BEN_SET_DET, com.optum.exts.cdb.model.PLN_BEN_SET_DET> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (value != null) {
                                    plnBenSetDetWriteTransformer.insertData(key, value);
                                    log.info("PLN_BEN_SET_DET written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                } else {
                                    plnBenSetDetWriteTransformer.delete(key);
                                    log.info("PLN_BEN_SET_DET physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                }
                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {
                        log.error("Exception",throwable);

                        log.error("SQLException in Table PLN_BEN_SET_DET :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                        log.error("SQLException in Table PLN_BEN_SET_DET :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());


                        //throw new RuntimeException("");
                        log.error("Restarting PLN_BEN_SET_DET POD"); System.exit(1);
                        //log.error("Exception",throwable);
                    }
                }
                return KeyValue.pair(key, value);

            }).transform(new MetadataTransformer());
        } catch (Exception e) {
            log.error("Exception in Table PLN_BEN_SET_DET ::" + memberKStream.transform(new MetadataTransformer()));
            e.printStackTrace();
            log.error("Restarting PLN_BEN_SET_DET POD"); System.exit(1);
        }


        return memberKStream;
    }





   @Bean
    public KStream<POL_INFO, com.optum.exts.cdb.model.POL_INFO> polInfoStream(StreamsBuilder builder) {

       if (!consumerUtil.deploymentFlag("POL_INFO")) {

           return null;
       }
       log.info("::::::::::::::::::::::::::::::::::::::::POL_INFO Consumer is Activated ::::::::::::::::::::::::::::");
      // final int[] retryCount = {0};
//log.info("+++++++++++++++++++++++"+topicConfig.getAutoOffsetReset());

        KStream<POL_INFO, com.optum.exts.cdb.model.POL_INFO>
                memberKStream = builder.stream(topicConfig.getPolInfo());

        try {
            KStream stream = memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("POL_INFO Consuming key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                try {
                    if (value != null) {
                        polInfoWriteTransformer.insertData(key, value);
                        log.info("POL_INFO written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                    } else {

                        polInfoWriteTransformer.delete(key);
                        log.info("POL_INFO physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                    }
                } catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<POL_INFO, com.optum.exts.cdb.model.POL_INFO>, Exception>() {
                            @Override
                            public KeyValue<POL_INFO, com.optum.exts.cdb.model.POL_INFO> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (value != null) {
                                    polInfoWriteTransformer.insertData(key, value);
                                    log.info("POL_INFO written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                } else {
                                    polInfoWriteTransformer.delete(key);
                                    log.info("POL_INFO physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                }
                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {
                        log.error("Exception",throwable);

                        log.error("SQLException in Table POL_INFO :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                        log.error("SQLException in Table POL_INFO :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());


                        //throw new RuntimeException("");
                        log.error("Restarting POL_INFO POD"); System.exit(1);
                        //log.error("Exception",throwable);
                    }
                }
                return KeyValue.pair(key, value);

            }).transform(new MetadataTransformer());
        } catch (Exception e) {
            log.error("Exception in Table POL_INFO ::" + memberKStream.transform(new MetadataTransformer()));
            e.printStackTrace();
            log.error("Restarting POL_INFO POD"); System.exit(1);
        }


        return memberKStream;
    }




    @Bean
    public KStream<RX_BEN_SET_DET, com.optum.exts.cdb.model.RX_BEN_SET_DET> rxBenSetDetStream(StreamsBuilder builder) {
        if (!consumerUtil.deploymentFlag("RX_BEN_SET_DET")) {

            return null;
        }
        log.info("::::::::::::::::::::::::::::::::::::::::RX_BEN_SET_DET Consumer is Activated ::::::::::::::::::::::::::::");



        KStream<RX_BEN_SET_DET, com.optum.exts.cdb.model.RX_BEN_SET_DET>
                memberKStream = builder.stream(topicConfig.getRxBenSetDet());

        try {
            KStream stream = memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("RX_BEN_SET_DET Consuming key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                try {
                    if (value != null) {
                        rxBenSetDetWriteTransformer.insertData(key, value);
                        log.info("RX_BEN_SET_DET written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                    } else {

                        rxBenSetDetWriteTransformer.delete(key);
                        log.info("RX_BEN_SET_DET physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                    }
                } catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<RX_BEN_SET_DET, com.optum.exts.cdb.model.RX_BEN_SET_DET>, Exception>() {
                            @Override
                            public KeyValue<RX_BEN_SET_DET, com.optum.exts.cdb.model.RX_BEN_SET_DET> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (value != null) {
                                    rxBenSetDetWriteTransformer.insertData(key, value);
                                    log.info("RX_BEN_SET_DET written key  :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                } else {
                                    rxBenSetDetWriteTransformer.delete(key);
                                    log.info("RX_BEN_SET_DET physical delete key +++ :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                                }
                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {
                        log.error("Exception",throwable);

                        log.error("SQLException in Table RX_BEN_SET_DET :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                        log.error("SQLException in Table RX_BEN_SET_DET :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());


                        //throw new RuntimeException("");
                        log.error("Restarting RX_BEN_SET_DET POD"); System.exit(1);
                        //log.error("Exception",throwable);
                    }
                }



                return KeyValue.pair(key, value);

            }).transform(new MetadataTransformer());
        } catch (Exception e) {
            log.error("SQLException in Table RX_BEN_SET_DET ::" + memberKStream.transform(new MetadataTransformer()));
            e.printStackTrace();
            log.error("Restarting RX_BEN_SET_DET POD"); System.exit(1);
        }


        return memberKStream;
    }



}








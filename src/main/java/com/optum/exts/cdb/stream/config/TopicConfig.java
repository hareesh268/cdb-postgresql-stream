package com.optum.exts.cdb.stream.config;




import com.google.common.collect.ImmutableMap;
import com.optum.exts.common.stream.config.KafkaStreamsConfig;
import com.optum.exts.common.stream.serde.ExtsSpecificAvroSerde;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import javax.validation.constraints.NotNull;
import java.util.Map;

//import io.confluent.kafka.streams.serdes.avro.

/**
 * Created by rgupta59
 */

@Configuration
@ConfigurationProperties(prefix = "streams.topicinfo")
public class TopicConfig extends KafkaStreamsConfig {


    @NotNull
    private String cnsmCustDefnFld;
    @NotNull
    private String cnsmEft;
    @NotNull
    private String cnsmMdcrElig;
    @NotNull
    private String cnsmMdcrPrimacy;
    @NotNull
    private String cnsmPrxstCond;
    @NotNull
    private String cnsmSlryBasDedOop;

    @NotNull
    String covInfo;
    @NotNull
    String custInfo;
    @NotNull
    String plnBenSet;
    @NotNull
    String plnBenSetDet;
    @NotNull
    String polInfo;
    @NotNull
    String rxBenSetDet;

    @NotNull
    private String cnsmAuthRep;
    @NotNull
    private String cnsmCal;
    @NotNull
    private String cnsmCobPrimacy;
    @NotNull
    private String cnsmCobPrisec;
    @NotNull
    private String cnsmCovCustDefnFld;

    @NotNull
    String lcnsmSrch;
    @NotNull
    String cnsmSts;
    @NotNull
    String lcovPrdtdt;
    @NotNull
    String covLvlTyp;
    @NotNull
    String mlCnsmAdr;
    @NotNull
    String mlCnsmTel;
    @NotNull
    String mlCnsmElctrAdr;
    @NotNull
    String lCovPrdtPcp;
    @NotNull
    String cnsmDtl;
    @NotNull
    String lHltSrvDt;
    @NotNull
    String lLfDisPrdtDt;
    @NotNull
    String cdbSecurity;
    @NotNull
    String cnsmMdcrEnrl;
    @NotNull
    String cnsmMdcrPrisec;
    @NotNull
    String mlCnsmXref;

    @NotNull
    String prdtTrig;

    @NotNull
    String disTrig;

    @NotNull
    String hltSrvTrig;

    @NotNull
    String lCnsmSrchTrig;

    @NotNull
    String cnsmOthrIns;

    @NotNull
    String cnsmMdcrEntl;


    public String getCnsmOthrIns() {
        return cnsmOthrIns;
    }

    public void setCnsmOthrIns(String cnsmOthrIns) {
        this.cnsmOthrIns = cnsmOthrIns;
    }

    public String getCnsmMdcrEntl() {
        return cnsmMdcrEntl;
    }

    public void setCnsmMdcrEntl(String cnsmMdcrEntl) {
        this.cnsmMdcrEntl = cnsmMdcrEntl;
    }

    public String getDisTrig() {
        return disTrig;
    }

    public void setDisTrig(String disTrig) {
        this.disTrig = disTrig;
    }

    public String getHltSrvTrig() {
        return hltSrvTrig;
    }

    public void setHltSrvTrig(String hltSrvTrig) {
        this.hltSrvTrig = hltSrvTrig;
    }

    public String getPrdtTrig() {
        return prdtTrig;
    }

    public void setPrdtTrig(String prdtTrig) {
        this.prdtTrig = prdtTrig;
    }

    public String getlCnsmSrchTrig() {
        return lCnsmSrchTrig;
    }

    public void setlCnsmSrchTrig(String lCnsmSrchTrig) {
        this.lCnsmSrchTrig = lCnsmSrchTrig;
    }

    public String getLcovPrdtdt() {
        return lcovPrdtdt;
    }

    public void setLcovPrdtdt(String lcovPrdtdt) {
        this.lcovPrdtdt = lcovPrdtdt;
    }

    public String getCovLvlTyp() {
        return covLvlTyp;
    }

    public void setCovLvlTyp(String covLvlTyp) {
        this.covLvlTyp = covLvlTyp;
    }

    public String getMlCnsmAdr() {
        return mlCnsmAdr;
    }

    public void setMlCnsmAdr(String mlCnsmAdr) {
        this.mlCnsmAdr = mlCnsmAdr;
    }

    public String getMlCnsmTel() {
        return mlCnsmTel;
    }

    public void setMlCnsmTel(String mlCnsmTel) {
        this.mlCnsmTel = mlCnsmTel;
    }

    public String getMlCnsmElctrAdr() {
        return mlCnsmElctrAdr;
    }

    public void setMlCnsmElctrAdr(String mlCnsmElctrAdr) {
        this.mlCnsmElctrAdr = mlCnsmElctrAdr;
    }

    public String getlCovPrdtPcp() {
        return lCovPrdtPcp;
    }

    public void setlCovPrdtPcp(String lCovPrdtPcp) {
        this.lCovPrdtPcp = lCovPrdtPcp;
    }

    public String getCnsmDtl() {
        return cnsmDtl;
    }

    public void setCnsmDtl(String cnsmDtl) {
        this.cnsmDtl = cnsmDtl;
    }

    public String getlHltSrvDt() {
        return lHltSrvDt;
    }

    public void setlHltSrvDt(String lHltSrvDt) {
        this.lHltSrvDt = lHltSrvDt;
    }

    public String getlLfDisPrdtDt() {
        return lLfDisPrdtDt;
    }

    public void setlLfDisPrdtDt(String lLfDisPrdtDt) {
        this.lLfDisPrdtDt = lLfDisPrdtDt;
    }

    public String getCdbSecurity() {
        return cdbSecurity;
    }

    public void setCdbSecurity(String cdbSecurity) {
        this.cdbSecurity = cdbSecurity;
    }

    public String getCnsmMdcrEnrl() {
        return cnsmMdcrEnrl;
    }

    public void setCnsmMdcrEnrl(String cnsmMdcrEnrl) {
        this.cnsmMdcrEnrl = cnsmMdcrEnrl;
    }

    public String getCnsmMdcrPrisec() {
        return cnsmMdcrPrisec;
    }

    public void setCnsmMdcrPrisec(String cnsmMdcrPrisec) {
        this.cnsmMdcrPrisec = cnsmMdcrPrisec;
    }

    public String getMlCnsmXref() {
        return mlCnsmXref;
    }

    public void setMlCnsmXref(String mlCnsmXref) {
        this.mlCnsmXref = mlCnsmXref;
    }

    public String getCnsmAuthRep() {
        return cnsmAuthRep;
    }

    public void setCnsmAuthRep(String cnsmAuthRep) {
        this.cnsmAuthRep = cnsmAuthRep;
    }
    public String getCnsmCal() {
        return cnsmCal;
    }

    public void setCnsmCal(String cnsmCal) {
        this.cnsmCal = cnsmCal;
    }

    public String getCnsmCobPrimacy() {
        return cnsmCobPrimacy;
    }

    public void setCnsmCobPrimacy(String cnsmCobPrimacy) {
        this.cnsmCobPrimacy = cnsmCobPrimacy;
    }
    public String getCnsmCobPrisec() {
        return cnsmCobPrisec;
    }

    public void setCnsmCobPrisec(String cnsmCobPrisec) {
        this.cnsmCobPrisec = cnsmCobPrisec;
    }


    public String getCnsmCovCustDefnFld() {
        return cnsmCovCustDefnFld;
    }

    public void setCnsmCovCustDefnFld(String cnsmCovCustDefnFld) {
        this.cnsmCovCustDefnFld = cnsmCovCustDefnFld;
    }
    public String getLcnsmSrch() {
        return lcnsmSrch;
    }

    public void setLcnsmSrch(String lcnsmSrch) {
        this.lcnsmSrch = lcnsmSrch;
    }

    public String getCnsmSts() {
        return cnsmSts;
    }

    public void setCnsmSts(String cnsmSts) {
        this.cnsmSts = cnsmSts;
    }

    public String getCnsmEft() {
        return cnsmEft;
    }

    public void setCnsmEft(String cnsmEft) {
        this.cnsmEft = cnsmEft;
    }

    public String getcnsmMdcrElig() {
        return cnsmMdcrElig;
    }

    public void setcnsmMdcrElig(String cnsmMdcrElig) {
        this.cnsmMdcrElig = cnsmMdcrElig;
    }

    public String getcnsmMdcrPrimacy() {
        return cnsmMdcrPrimacy;
    }

    public void setcnsmMdcrPrimacy(String cnsmMdcrPrimacy) {
        this.cnsmMdcrPrimacy = cnsmMdcrPrimacy;
    }

    public String getcnsmPrxstCond() {
        return cnsmPrxstCond;
    }

    public void setcnsmPrxstCond(String cnsmPrxstCond) {
        this.cnsmPrxstCond = cnsmPrxstCond;
    }

    public String getCnsmSlryBasDedOop() {
        return cnsmSlryBasDedOop;
    }

    public void setCnsmSlryBasDedOop(String cnsmSlryBasDedOop) {
        this.cnsmSlryBasDedOop = cnsmSlryBasDedOop;
    }

    public String getcnsmCustDefnFld() {
        return cnsmCustDefnFld;
    }

    public void setcnsmCustDefnFld(String cnsmCustDefnFld) {
        this.cnsmCustDefnFld = cnsmCustDefnFld;
    }

    public String getCovInfo() {
        return covInfo;
    }

    public void setCovInfo(String covInfo) {
        this.covInfo = covInfo;
    }

    public String getCustInfo() {
        return custInfo;
    }

    public void setCustInfo(String custInfo) {
        this.custInfo = custInfo;
    }

    public String getPlnBenSet() {
        return plnBenSet;
    }

    public void setPlnBenSet(String plnBenSet) {
        this.plnBenSet = plnBenSet;
    }

    public String getPlnBenSetDet() {
        return plnBenSetDet;
    }

    public void setPlnBenSetDet(String plnBenSetDet) {
        this.plnBenSetDet = plnBenSetDet;
    }

    public String getPolInfo() {
        return polInfo;
    }

    public void setPolInfo(String polInfo) {
        this.polInfo = polInfo;
    }

    public String getRxBenSetDet() {
        return rxBenSetDet;
    }

    public void setRxBenSetDet(String rxBenSetDet) {
        this.rxBenSetDet = rxBenSetDet;
    }

    @Override
    protected Map<String, Object> moreConfigs() {
        //default serdes
        return ImmutableMap.of(
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName(),
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, ExtsSpecificAvroSerde.class
        );
    }
}
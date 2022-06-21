package com.optum.exts.cdb.stream.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.List;

/**
 * Created by rgupta59
 */
@Configuration
@ConfigurationProperties(prefix = "input")
public class UtilityConfig {
    @Valid
    private  HashMap<String, String> queryLookup;


    @Valid
    private  HashMap<String,List<String>> groupList;


    @NotNull
    String schema;

    @NotNull
    String physicalDelValue;

    @NotNull
    String srcSysId;

    public HashMap<String, List<String>> getGroupList() {
        return groupList;
    }

    public void setGroupList(HashMap<String, List<String>> groupList) {
        this.groupList = groupList;
    }

    public String getPhysicalDelValue() {
        return physicalDelValue;
    }

    public void setPhysicalDelValue(String physicalDelValue) {
        this.physicalDelValue = physicalDelValue;
    }

    public String getSrcSysId() {
        return srcSysId;
    }

    public void setSrcSysId(String srcSysId) {
        this.srcSysId = srcSysId;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }



    public HashMap<String, String> getQueryLookup() {
        return queryLookup;
    }

    public void setQueryLookup(HashMap<String, String> queryLookup) {
        this.queryLookup = queryLookup;
    }
}
package com.optum.exts.cdb.stream.utility;


import com.optum.exts.cdb.stream.config.UtilityConfig;

import org.apache.commons.lang.StringUtils;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;



import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;



/**
 * Created by rgupta59
 */
@Configuration
public class ConsumerUtil {
    @Autowired
    UtilityConfig utilityConfig;

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(ConsumerUtil.class);

    public Date dateParser(String strDate) throws SQLException {
        boolean isEmpty = strDate == null || strDate.trim().length() == 0;

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        //String dateInString = "7-Jun-2013";

        try { if (isEmpty) {
            // handle the validation
            return new Date(formatter.parse("0002-01-01").getTime());
        }

            Date date = new Date(formatter.parse(strDate).getTime());

            return date;

        }  catch (java.text.ParseException e) {
            log.error("dateParser Exception");

            throw new SQLException();

        }
        //return null;

}



    public Timestamp timeStampParser(String strtTimestmp) throws SQLException {
        try {


            String[] dateTime= strtTimestmp.replace("T"," ").split("\\.");


            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");

            String formatted = dateTime[0].replace("T"," ")+"."+ ((dateTime[1].length() > 6) ? dateTime[1].substring(0,6) : StringUtils.rightPad(dateTime[1],6,"0") );

            Timestamp reqTimeStamp = Timestamp.valueOf(formatted);


            return reqTimeStamp;
        }
        catch(Exception e) { //this generic but you can control another types of exception
            log.error("timeStampParser Exception");

            throw new SQLException();


        }


       // return null;

    }


    public Timestamp getsysTimeStamp() throws SQLException{
        try {


            DateFormat genericFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSsss");
            genericFormat.setTimeZone(TimeZone.getTimeZone("CST"));
            String formatted = genericFormat.format(new java.util.Date());
            Timestamp reqTimeStamp = Timestamp.valueOf(formatted);


            return reqTimeStamp;
        } catch(Exception e) { //this generic but you can control another types of exception
            log.error("sysTimeStamp Exception");

            throw new SQLException();



        }
        //return null;

    }

    public boolean deploymentFlag(String var)
    {
        String groupAssigned = "default";

        for (Map.Entry<String,List<String>> entry :  utilityConfig.getGroupList().entrySet())
            if( entry.getValue().contains(var)) {
                groupAssigned= entry.getKey();
                break;
            }

        String prefix = System.getenv().getOrDefault("GROUP", "default");
        return prefix.equalsIgnoreCase(groupAssigned) ?  true : false;


    }


    public String removeAsciNull(String data) {

        boolean isEmpty = data == null || data.trim().length() == 0;
        if (isEmpty) {

            return "";
        }
        else {
            return data.replace("\u0000", "").replaceAll("[^\\u0000-\\u007F\\u0900-\\u097f]", "").trim();
        }

    }



}

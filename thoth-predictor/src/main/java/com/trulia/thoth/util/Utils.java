package com.trulia.thoth.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * User: dbraga - Date: 10/4/14
 */
public class Utils {

  public static String getThothSampledFileName(String mergeDirectory){
    DateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd");
    Date date = new Date();
    return mergeDirectory +  dateFormat.format(date)+"_merged";
  }
}

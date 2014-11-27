package com.trulia.thoth;

import com.trulia.thoth.pojo.QueryPojo;

/**
 * User: dbraga - Date: 11/27/14
 */
public class Converter {

  public static QueryPojo tsvToQueryPojo(String line){


    String[] fields = line.split("\t");
    if (fields.length != 7) return null; //TODO: too specific, need to make it generic



    QueryPojo queryPojo = new QueryPojo();
    queryPojo.setParams(fields[3]);
    if (!fields[4].isEmpty()) queryPojo.setQtime(fields[4]);
    if (!fields[5].isEmpty()) queryPojo.setHits(fields[5]);
    queryPojo.setBitmask(fields[6]);
    return queryPojo;
  }
}

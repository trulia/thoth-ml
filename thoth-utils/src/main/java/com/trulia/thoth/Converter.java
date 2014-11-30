package com.trulia.thoth;

import com.trulia.thoth.pojo.QueryPojo;
import com.trulia.thoth.pojo.QuerySamplingDetails;
import org.apache.solr.common.SolrDocument;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * User: dbraga - Date: 11/27/14
 */
public class Converter {
  private static final String[] samplingFields = { "hostname_s", "pool_s", "source_s", "params_s", "qtime_i", "hits_i", "bitmask_s"};
  private static final Pattern FACET_PATTERN = Pattern.compile("facet=true");

  private static Pattern RANGE_QUERY_PATTERN = Pattern.compile("\\w*:\\[(.*?TO.*?)\\]");
  private static Pattern COLLAPSING_SEARCH_PATTERN = Pattern.compile("collapse.field=");
  private static Pattern GEOSPATIAL_PATTERN = Pattern.compile("!spatial");
  private static Pattern OPEN_HOMES_PATTERN = Pattern.compile("ohDay_ms:\\[");


  /**
   * Given a tab separated value line, it will create a query pojo
   * @param line something like
   * server pool typeOfRequest json qtime hits bitmask
   * @return
   *
   */
  public static QueryPojo tsvToQueryPojo(String line){
    String[] fields = line.split("\t");
    if (fields.length != 7) return null; //TODO: too specific, need to make it generic || static somewhere

    QueryPojo queryPojo = new QueryPojo();
    queryPojo.setParams(fields[3]);
    if (!fields[4].isEmpty()) queryPojo.setQtime(fields[4]);
    if (!fields[5].isEmpty()) queryPojo.setHits(fields[5]);
    queryPojo.setBitmask(fields[6]);
    return queryPojo;
  }

  public static String writeValueOrEmptyString(SolrDocument doc, String fieldName){
    if (doc.containsKey(fieldName)) return doc.getFieldValue(fieldName).toString();
    else return "";
  }

  private static boolean checkForMatch(Pattern pattern, String query){
    Matcher matcher = pattern.matcher(query);
    if (matcher.find()) return true;
    else return false;
  }

  public static String thothDocToTsv(SolrDocument doc, ObjectMapper mapper) throws IOException {
    String tsvLine = "";

    for (String fieldName: samplingFields){

      if ("params_s".equals(fieldName)){
        String extractedDetails = extractDetailsFromParams(writeValueOrEmptyString(doc, fieldName), mapper);
        if (!"".equals(extractedDetails)) tsvLine += extractedDetails;
      } else {
        tsvLine += writeValueOrEmptyString(doc,fieldName);
      }
      tsvLine += "\t";

    }
    tsvLine += "\n";
    return tsvLine;
  }

  public static String extractDetailsFromParams(String params, ObjectMapper mapper) throws IOException {
    String[] splitted = params.replaceAll("\\{","").replaceAll("\\}", "").split("&");
    if (splitted.length < 1) return "";

    QuerySamplingDetails querySamplingDetails = new QuerySamplingDetails();

    QuerySamplingDetails.Details details = new QuerySamplingDetails.Details();
    QuerySamplingDetails.Feature features = new QuerySamplingDetails.Feature();

    features.setFacet(checkForMatch(FACET_PATTERN, params));
    features.setCollapsingSearch(checkForMatch(COLLAPSING_SEARCH_PATTERN, params));
    features.setContainsOpenHomes(checkForMatch(OPEN_HOMES_PATTERN, params));
    features.setGeospatialSearch(checkForMatch(GEOSPATIAL_PATTERN, params));
    features.setRangeQuery(checkForMatch(RANGE_QUERY_PATTERN, params));


    for (String s : splitted){
      if ("".equals(s)) continue;
      String[] elements = s.split("=");
      if (elements.length == 2) {
        String k = elements[0];
        String v = elements[1];
        if ("start".equals(k)) details.setStart(v);
        else if ("rows".equals(k)) details.setRows(v);
        else if ("q".equals(k)) details.setQuery(v);
        else if ("fq".equals(k)) details.setFilterQuery(v);
        else if ("sort".equals(k)) details.setSort(v);
        else if ("slowpool".equals(k)) details.setSlowpool(v);
        else if ("collapse.field".equals(k)) details.setCollapseField(v);
        else if ("collapse.includeCollapsedDocs.fl".equals(k)) details.setCollapseDocFl(v);
        else if ("facet.field".equals(k)) details.setFacetField(v);
        else if ("facet.zeros".equals(k)) details.setFacetZeros(v);
        else if ("ghl".equals(k)) details.setGhl(v);


        else if ( !("cachebust".equals(k)) && !("wt".equals(k)) && !("version".equals(k)) && !("version".equals(k)) && !("fl".equals(k)) ) {
          // want to know what i'm missing
          System.out.println("Missing field (" +k+ ")  value ("+v+")");
        }

      } else if (details.getQuery() == null && features.isGeospatialSearch() && s.contains("spatial")){
        elements = s.split("!spatial ");
        details.setQuery(elements[1]);
      }
      else System.out.println("Not recognized k,v element. from " + s);
    }



    querySamplingDetails.setDetails(details);
    querySamplingDetails.setFeatures(features);
    //System.out.println("JSON: " + mapper.writeValueAsString(querySamplingDetails));
    return mapper.writeValueAsString(querySamplingDetails);

  }

  public static void main(String[] args){
    System.out.println(QueryPojo.class.getDeclaredFields().length);
  }
}

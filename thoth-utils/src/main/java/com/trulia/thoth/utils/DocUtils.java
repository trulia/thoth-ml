package com.trulia.thoth.utils;

import com.trulia.thoth.pojo.QuerySamplingDetails;
import org.apache.solr.common.SolrDocument;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * User: dbraga - Date: 12/6/14
 */
public class DocUtils {

  private static final Pattern FACET_PATTERN = Pattern.compile("facet=true");
  private static Pattern RANGE_QUERY_PATTERN = Pattern.compile("\\w*:\\[(.*?TO.*?)\\]");
  private static Pattern COLLAPSING_SEARCH_PATTERN = Pattern.compile("collapse.field=");
  private static Pattern GEOSPATIAL_PATTERN = Pattern.compile("!spatial");

  /**
   * Get value from field of a thoth doc
   * @param doc thoth document
   * @param fieldName name of the field
   * @return value or empty string
   */
  public static String writeValueOrEmptyString(SolrDocument doc, String fieldName){
    if (doc.containsKey(fieldName)) return doc.getFieldValue(fieldName).toString();
    else return "";
  }

  /**
   * Pattern match against line
   * @param pattern pattern to check
   * @param line to check against
   * @return true if pattern matched, false otherwise
   */
  private static boolean checkForMatch(Pattern pattern, String line){
    Matcher matcher = pattern.matcher(line);
    if (matcher.find()) return true;
    else return false;
  }


  /**
   * Transform a params string to a params array
   * @param params string representation of params
   * @return array of params
   */
  private static String[] getParamsArray(String params){
    return params.replaceAll("\\{","").replaceAll("\\}", "").split("&");
  }


  //TODO: refactor better and move
  public static String extractDetailsFromParams(String params, ObjectMapper mapper) throws IOException {
    String[] split = getParamsArray(params);
    if (split.length < 1) return "";
    QuerySamplingDetails querySamplingDetails = new QuerySamplingDetails();
    QuerySamplingDetails.Details details = new QuerySamplingDetails.Details();
    QuerySamplingDetails.Feature features = new QuerySamplingDetails.Feature();
    features.setFacet(checkForMatch(FACET_PATTERN, params));
    features.setCollapsingSearch(checkForMatch(COLLAPSING_SEARCH_PATTERN, params));
    features.setGeospatialSearch(checkForMatch(GEOSPATIAL_PATTERN, params));
    features.setRangeQuery(checkForMatch(RANGE_QUERY_PATTERN, params));

    for (String s : split){
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
        else if ( !("wt".equals(k))  && !("version".equals(k)) && !("fl".equals(k)) ) {
          // Log what we didn't extract
          System.out.println("Missing extracted field (" +k+ ")  value ("+v+")");
        }
      } else if (details.getQuery() == null && features.isGeospatialSearch() && s.contains("spatial")){
        elements = s.split("!spatial ");
        details.setQuery(elements[1]);
      }
      else System.out.println("Not recognized k,v element. from " + s);
    }

    querySamplingDetails.setDetails(details);
    querySamplingDetails.setFeatures(features);
    return mapper.writeValueAsString(querySamplingDetails);

  }

}

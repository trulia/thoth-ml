package com.trulia.thoth;


import com.trulia.thoth.pojo.QuerySamplingDetails;
import com.trulia.thoth.pojo.QuerySamplingDetails2;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.util.StringUtils;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * User: dbraga - Date: 12/5/14
 */
public class Test {


  /*
   bitMask

   [0] = containsRangeQuery
   [1] = isFacetingSearch
   [2] = isPropertyLookup
   [3] = isPropertyHashLookup
   [4] = isCollapsingSearch
   [5] = isGeospatialSearch
   [6] = containsOpenHomes

*/

  static Set<String> fields;
  static Map<String,String> mapping;

  public static void main(String[] args) throws IOException {
    File fileToFix = new File("/Users/dbraga/test_data");
    BufferedReader br = new BufferedReader(new FileReader(fileToFix));

    BufferedWriter bw = new BufferedWriter(new PrintWriter("/tmp/done"));

    fields = new HashSet<String>();
    mapping = new HashMap<String, String>();

    String line = br.readLine();
    while (line != null) {
      try {
        line = br.readLine();
        if (line == null) {
          System.out.println("line not valid " + line);
          continue;
        }
        if (line == "") {
          System.out.println("line not valid " + line);
          continue;
        }

        String[] split = line.split("\t");
        if (split.length != 7) {
          System.out.println("line not valid " + line);
          continue;
        }

        // name of server
        split[0] = "demo-host";
        // Pool
        split[1] = "demo-pool";
        // Query type
        split[2] = "SolrQuery";
        // Query
        //      split[3] ;
        // bitmask

        if (split[6].length() != 7) {
          System.out.println("bitmask not valid " + line);
          continue;
        }

        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(SerializationConfig.Feature.WRITE_NULL_PROPERTIES, false);

        QuerySamplingDetails querySamplingDetails = mapper.readValue(split[3], QuerySamplingDetails.class);

        QuerySamplingDetails.Feature feature = querySamplingDetails.getFeatures();
        QuerySamplingDetails2.Feature feature1 = new QuerySamplingDetails2.Feature();
        feature1.setCollapsingSearch(feature.isCollapsingSearch());
        feature1.setFacet(feature.isFacet());
        feature1.setGeospatialSearch(feature.isGeospatialSearch());
        feature1.setRangeQuery(feature.isRangeQuery());

        QuerySamplingDetails.Details details = querySamplingDetails.getDetails();
        QuerySamplingDetails2.Details details1 = new QuerySamplingDetails2.Details();

        details1.setCollapseDocFl(details.getCollapseDocFl());
        details1.setCollapseField(details.getCollapseField());
        details1.setFacetField(details.getFacetField());
        details1.setFacetZeros(details.getFacetZeros());

        String fq = null;
        if (details.getFilterQuery() != null)
        fq = removeTruliaFieldValues(removeTruliaFieldNames(details.getFilterQuery()));

        details1.setFilterQuery(fq);

        String query = null;
        if (details.getQuery() != null)
          query = removeTruliaFieldValues(removeTruliaFieldNames(details.getQuery()));

        details1.setQuery(query);

        details1.setRows(String.valueOf(details.getRows()));
        details1.setSlowpool(String.valueOf(details.isSlowpool()));
        details1.setSort(details.getSort());
        details1.setStart(String.valueOf(details.getStart()));





        QuerySamplingDetails2 querySamplingDetails2 = new QuerySamplingDetails2();
        querySamplingDetails2.setDetails(details1);
        querySamplingDetails2.setFeatures(feature1);

        split[3] = mapper.writeValueAsString(querySamplingDetails2);
        //
        //      QuerySamplingDetails.Details details = new QuerySamplingDetails.Details();
        //      QuerySamplingDetails.Feature features = new QuerySamplingDetails.Feature();


        split[6] = String.valueOf(split[6].charAt(0)) + String.valueOf(split[6].charAt(1)) + String.valueOf(split[6].charAt(4)) + String.valueOf(split[6].charAt(5));


        ArrayList<String> list = new ArrayList<String>();
        org.apache.commons.collections.CollectionUtils.addAll(list, split);
        line = StringUtils.join("\t", list);

//        for (String el: fields){
//          System.out.println(el);
//        }
//        System.out.println(line);
        bw.write(line );
        bw.newLine();

      } catch (Exception ignored){
        ignored.printStackTrace();
      }
    }

    bw.close();
  }

  private static String removeTruliaFieldValues(String query){
    query = query.replaceAll("\\+"," ");
    Matcher m = Pattern.compile("\"([\\w]*[ ]?[\\w]*)\"").matcher(query);
    while (m.find()) {
      String el = m.group();
      query = query.replaceAll(el, RandomStringUtils.randomAlphabetic(5));
      }
    return query;
  }

  private static String removeTruliaFieldNames(String query){
    Matcher m = Pattern.compile("[\\w]*_[\\w]").matcher(query);
    while (m.find()) {
      String el = m.group();
      if (!fields.contains(el)){
        fields.add(el);
        mapping.put(el,RandomStringUtils.randomAlphabetic(5)+"_"+el.split("_")[1]);
      }
    }
    for (Map.Entry<String,String> entry : mapping.entrySet()){
      query = query.replaceAll(entry.getKey(),entry.getValue());
    }
    return query;
  }


}

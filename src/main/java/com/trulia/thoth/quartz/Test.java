package com.trulia.thoth.quartz;

import com.trulia.thoth.pojo.QuerySamplingDetails;
import com.trulia.thoth.pojo.ServerDetail;
import com.trulia.thoth.util.ThothServers;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * User: dbraga - Date: 10/13/14
 */
public class Test {



  private static final Pattern FACET_PATTERN = Pattern.compile("facet=true");

  private static Pattern RANGE_QUERY_PATTERN = Pattern.compile("\\w*:\\[(.*?TO.*?)\\]");
  private static Pattern COLLAPSING_SEARCH_PATTERN = Pattern.compile("collapse.field=");
  private static Pattern GEOSPATIAL_PATTERN = Pattern.compile("!spatial");
  private static Pattern OPEN_HOMES_PATTERN = Pattern.compile("ohDay_ms:\\[");

  private static boolean checkForMatch(Pattern pattern, String query){
    Matcher matcher = pattern.matcher(query);
    if (matcher.find()) return true;
    else return false;
  }


  public static String extractDetailsFromParams(String params) throws IOException {

    ObjectMapper mapper = new ObjectMapper();

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
        try { if ("start".equals(k)) details.setStart(v);} catch (Exception e) {System.out.println(e);};
        if ("rows".equals(k)) details.setRows(v);
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


  public static void main(String[] args) throws SolrServerException, IOException {
    File f = new File("/tmp/exceptions");
    BufferedWriter bw = new BufferedWriter(new PrintWriter(f));


    HttpSolrServer thothIndex = new HttpSolrServer("http://thoth:8983/solr/collection1");
    ThothServers thothServers = new ThothServers();

    ArrayList<ServerDetail> serversDetail = thothServers.getList(thothIndex);
    //ArrayList<ServerDetail> serversDetail = new ArrayList<ServerDetail>();

    //serversDetail.add(new ServerDetail("search501","bot","8050","active"));
    for (ServerDetail server: serversDetail){
      String hostname = server.getName();
      String port = server.getPort();
      String pool = server.getPool();
      String core = server.getCore();

      String[] samplingFields = { "hostname_s", "pool_s", "params_s", "qtime_i", "hits_i", "bitmask_s","stackTrace_s"};

      SolrQuery solrQuery = new SolrQuery("hostname_s:"+hostname+" AND port_i:"+port+" AND pool_s:"+pool+" AND coreName_s:"+core+" AND exception_b:true" );
      solrQuery.setSort(new SolrQuery.SortClause("timestamp_dt", SolrQuery.ORDER.desc));
      solrQuery.setRows(500);  // Returning 100 docs
      QueryResponse qr = thothIndex.query(solrQuery);
      SolrDocumentList solrDocumentList = qr.getResults();


      if (solrDocumentList.size() < 1){
          break;
      }


      //List<SolrDocument> sample = SamplerWorker.randomSample(solrDocumentList, 100); //Sampling 10
      List<SolrDocument> sample = solrDocumentList;

      for (SolrDocument doc: sample){
        for (String fieldName: samplingFields) {
          if ("params_s".equals(fieldName)) {
            String extractedDetails = extractDetailsFromParams(writeValueOrEmptyString(doc, fieldName));
            if (!"".equals(extractedDetails)) {

              bw.write(extractedDetails);
              bw.write("\t");
            }


          } else {
            if (fieldName.equals("stackTrace_s"))  bw.write("\t");
            bw.write(writeValueOrEmptyString(doc,fieldName));
            bw.write("\t");

            //System.out.println("Field(" + fieldName + ") - Value(" + writeValueOrEmptyString(doc, fieldName) + ")");
          }

        }


        bw.write("\n");
      }

    }










    bw.close();


  }

  public static String cleanStackTrace(String stackTrace){
    String output = stackTrace;
    Set<String> cleaned = new HashSet<String>();

    String[] splitted = output.split("\n");
    for (String line: splitted){


     if (line.contains("org.mortbay.jetty")) continue;
     line = line.split("\\(")[0];
      line = line.substring(line.lastIndexOf('.') + 1);
     cleaned.add(line);


    }

    output = StringUtils.join(cleaned.toArray()," ");


    return output.replaceAll("\t"," ").replaceAll("\n"," ");
  }


  public static String writeValueOrEmptyString(SolrDocument doc, String fieldName){
    if (doc.containsKey(fieldName)) {
      if (doc.containsKey("stackTrace_s")) return cleanStackTrace(doc.getFieldValue(fieldName).toString());
      else return doc.getFieldValue(fieldName).toString();

    }
    else return "";
  }


}

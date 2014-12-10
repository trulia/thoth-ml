package com.trulia.thoth;

/**
 * User: dbraga - Date: 12/6/14
 */

import com.trulia.thoth.classifier.Classifier;
import com.trulia.thoth.pojo.QueryPojo;
import com.trulia.thoth.requestdocuments.MessageRequestDocument;
import com.trulia.thoth.requestdocuments.SolrQueryRequestDocument;
import com.trulia.thoth.utils.DocUtils;
import org.apache.solr.common.SolrDocument;
import org.codehaus.jackson.map.ObjectMapper;
import java.io.IOException;

/**
 * User: dbraga - Date: 11/27/14
 */
public class Converter {
  private static final String[] samplingFields = { MessageRequestDocument.HOSTNAME, MessageRequestDocument.CORENAME, MessageRequestDocument.SOURCE, SolrQueryRequestDocument.PARAMS, SolrQueryRequestDocument.QTIME, SolrQueryRequestDocument.HITS, SolrQueryRequestDocument.BITMASK};

  /**
   * Given a tab separated value line, it will create a query pojo
   * @param line something like
   * server pool typeOfRequest json qtime hits bitmask
   * @return
   *
   */
  public static QueryPojo tsvToQueryPojo(String line){
    String[] fields = line.split("\t");
    if (fields.length != samplingFields.length) return null;
    QueryPojo queryPojo = new QueryPojo();
    queryPojo.setParams(fields[3]);
    if (!fields[4].isEmpty()) queryPojo.setQtime(fields[4]);
    if (!fields[5].isEmpty()) queryPojo.setHits(fields[5]);
    queryPojo.setBitmask(fields[6]);
    return queryPojo;
  }


   /**
   * Given a string representing a solr query, it will create a query pojo
   * @param solrQuery string representation of a solr query
   * @param hits number of hits
   * @param qtime qtime
   * @return query pojo
   */
  public static QueryPojo solrQueryToQueryPojo(String solrQuery, Integer hits, Integer qtime, ObjectMapper mapper) throws IOException {
    QueryPojo queryPojo = new QueryPojo();
    queryPojo.setParams(DocUtils.extractDetailsFromParams(solrQuery, mapper));
    queryPojo.setBitmask(new Classifier(solrQuery).createBitMask());
    if (qtime != null) queryPojo.setQtime(String.valueOf(qtime));
    if (hits != null ) queryPojo.setHits(String.valueOf(hits));
    return queryPojo;
  }

  /**
   * Represent important part of a thoth document into a tab separated value line
   * @param doc thoth document
   * @param mapper jackson mapper
   * @return tsv line
   * @throws IOException
   */
  public static String thothDocToTsv(SolrDocument doc, ObjectMapper mapper) throws IOException {
    String tsvLine = "";
    for (String fieldName: samplingFields){
      if (fieldName.equals(SolrQueryRequestDocument.PARAMS)){
        String extractedDetails = DocUtils.extractDetailsFromParams(DocUtils.writeValueOrEmptyString(doc, fieldName), mapper);
        if (!"".equals(extractedDetails)) tsvLine += extractedDetails;
      } else {
        tsvLine += DocUtils.writeValueOrEmptyString(doc, fieldName);
      }
      tsvLine += "\t";
    }
    tsvLine += "\n";
    return tsvLine;
  }

}

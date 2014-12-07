package com.trulia.thoth.quartz;

import com.trulia.thoth.Converter;
import com.trulia.thoth.pojo.ServerDetail;
import com.trulia.thoth.predictor.ModelHealth;
import com.trulia.thoth.requestdocuments.MessageRequestDocument;
import com.trulia.thoth.requestdocuments.SolrQueryRequestDocument;
import com.trulia.thoth.util.Utils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;

/**
 * User: dbraga - Date: 7/21/14
 */
public class SamplerWorker implements Callable<String>{
  private ServerDetail server;
  private BufferedWriter writer;
  private HttpSolrServer thothIndex;
  private ObjectMapper mapper;
  private String fileName;
  private static final int TOT_SAMPLE_COUNT = 100;
  private static final int RANDOM_SAMPLE_COUNT = TOT_SAMPLE_COUNT;
  private ModelHealth modelHealth;
  private static final String EXCEPTION = "exception_b";
  private static final int SLOW_FAST_QUERY_QTIME_THRESHOLD = 100;
  private String hostname;
  private String port;
  private String core;
  private String pool;

  public static <T> List<T> randomSample(List<T> items, int m){
    Random rnd = new Random();

    for(int i=0;i<m;i++){
      int pos = i + rnd.nextInt(items.size() - i);
      T tmp = items.get(pos);
      items.set(pos, items.get(i));
      items.set(i, tmp);
    }
    return items.subList(0, m);
  }

  public SamplerWorker(ServerDetail server, String samplingDirectory, ObjectMapper mapper, HttpSolrServer thothIndex, ModelHealth modelHealth) throws IOException {
    this.server = server;
    this.mapper = mapper;
    DateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd");
    Date date = new Date();
    this.fileName =  samplingDirectory + dateFormat.format(date) + "_" + server.getName();
    writer = new BufferedWriter(new FileWriter(new File(fileName), true));
    this.hostname = server.getName();
    this.pool = server.getPool();
    this.core = server.getCore();
    this.port = server.getPort();
    this.thothIndex = thothIndex;
    this.modelHealth = modelHealth;

  }


  /**
   * Takes care of updating the current model health
   * @param qtime qtime of a request
   * @param isDocumentIdentifiedAsSlow if the request got identified as slow
   * @param modelHealth the current model health
   */
  public void updateModelHealth(int qtime, boolean isDocumentIdentifiedAsSlow, ModelHealth modelHealth){
      if (qtime > SLOW_FAST_QUERY_QTIME_THRESHOLD){
        if (isDocumentIdentifiedAsSlow){
          modelHealth.incrementTruePositive();
        } else {
          modelHealth.incrementFalseNegative();
        }
      } else {
        if (isDocumentIdentifiedAsSlow){
          modelHealth.incrementFalsePositive();
        } else {
          modelHealth.incrementTrueNegative();
        }
      }
  }

  /**
   * Prepares the query used to get a sample of Thoth data
   * @return the solr query
   */
  private SolrQuery getSamplingSolrQuery(){
    // TODO: use same technique used in thoth core
    SolrQuery samplingSolrQuery = new SolrQuery(
          Utils.createFieldValueQuery(MessageRequestDocument.HOSTNAME, hostname) +
          " AND " + Utils.createFieldValueQuery(MessageRequestDocument.PORT, port) +
          " AND " + Utils.createFieldValueQuery(MessageRequestDocument.POOL, pool) +
          " AND " + Utils.createFieldValueQuery(MessageRequestDocument.CORENAME, core) +
          " AND NOT " + Utils.createFieldValueQuery(MessageRequestDocument.SOURCE, "WatchingRequest") +
          " AND NOT " + Utils.createFieldValueQuery(EXCEPTION, "true")
    );
    samplingSolrQuery.setSort(new SolrQuery.SortClause("timestamp_dt", SolrQuery.ORDER.desc));
    samplingSolrQuery.setRows(TOT_SAMPLE_COUNT);  // Returning TOT_SAMPLE_COUNT docs
    return samplingSolrQuery;
  }

  /**
   * Determines if prediction for given thoth document was correct or not and update the health score accordingly
   * @param doc thoth document
   */
  private void verifyQualityOfPrediction(SolrDocument doc){
    Integer qtime = (Integer) doc.getFieldValue(SolrQueryRequestDocument.QTIME);
    boolean isDocumentIdentifiedAsSlow = (Boolean) doc.getFieldValue("slowQuery_b") == true;
    updateModelHealth(qtime, isDocumentIdentifiedAsSlow, modelHealth);
  }

  /**
   * Take care of closing the file writer and printing out the action
   * @throws IOException
   */
  private void closeFileWriter() throws IOException {
    writer.close();
    System.out.println(fileName + " closed.");
  }

  @Override
  public String call() throws Exception {
    SolrDocumentList sampleOfThothDocs = thothIndex.query(getSamplingSolrQuery()).getResults();
    if (sampleOfThothDocs.size() < 1){
      System.out.println("ERROR: hostname: " + hostname+" returned 0 results. Skipping sampling" );
      closeFileWriter();
      return "skipped";
    }
    List<SolrDocument> randomSample = randomSample(sampleOfThothDocs, RANDOM_SAMPLE_COUNT);
    for (SolrDocument thothDocument: randomSample){
      verifyQualityOfPrediction(thothDocument);
      writer.write(Converter.thothDocToTsv(thothDocument, mapper));
    }
    closeFileWriter();
    return "done";
  }

}

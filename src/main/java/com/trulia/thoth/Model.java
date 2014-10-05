package com.trulia.thoth;

import com.trulia.thoth.pojo.QueryPojo;
import com.trulia.thoth.pojo.QuerySamplingDetails;
import com.trulia.thoth.util.Utils;
import net.sf.javaml.core.Dataset;
import net.sf.javaml.core.DefaultDataset;
import net.sf.javaml.core.SparseInstance;
import net.sf.javaml.tools.data.FileHandler;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.*;
import java.util.HashMap;
import java.util.Random;

/**
 * User: dbraga - Date: 10/1/14
 */

@Component
public class Model {
  private static final Logger LOG = Logger.getLogger(Model.class);
  private String version;
  static ObjectMapper mapper = new ObjectMapper();
  static HashMap<String, Integer> attributeIndex = new HashMap<String, Integer>();
  static int attributeCount = 0;
  static final int slowQueryThreshold = 50;
  @Value("${thoth.merging.dir}")
  private String mergeDirectory;
  @Value("${train.dataset.location}")
  private String exportedTrainDataset;
  @Value("${test.dataset.location}")
  private String exportedTestDataset;

  @PostConstruct
  public void init() {
    // Trying to fetch the version from file
    try {
      FileReader file = new FileReader("version");
      BufferedReader br = new BufferedReader(file);
      version = br.readLine();
    } catch (FileNotFoundException e) {
      // No version yet
      version = "-1";
    }
      catch (IOException e) {
      version = "-1";
    }
  }

  public void setVersion(String version) {
    this.version = version;
  }

  /**
   * Retrieve model version
   * @return String representation of the version
   */
  public String getVersion() {
    return version;
  }

  /**
   * Create a newly version using time since epoch and set it both on disk and in memory
   * @return newly created version
   */
  private String generateAndSetNewVersion(){
    // Fetch new version
    String newlyVersion = String.valueOf(System.currentTimeMillis());
    // Write new version to disk
    File f = new File("version");
    PrintWriter pw = null;
    try {
      pw = new PrintWriter(f);
      pw.write(newlyVersion);
      pw.close();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
    // Set new version in memory
    this.version = newlyVersion;
    return this.version;
  }

  /**
   * Train the model and return the newly model version
   * @return
   */
  public String trainModel(){
    // Actually train the model
    generateAndSetNewVersion();
    return version;
  }

  /**
   * Retrieve the model
   * @return model
   */
  public Object getModel(){
    return null;
  }

  /**
   * Generate dataset and train model
   * @throws IOException
   */
  public void generateDataSet() throws IOException {
   BufferedReader br = new BufferedReader(new FileReader(Utils.getThothSampledFileName(mergeDirectory)));
    // hostname_s, pool_s, source_s, params_s, qtime_i, hits_i, bitmask_s
    Dataset dataset = new DefaultDataset();
    // Training and test datasets
    Dataset train = new DefaultDataset();
    Dataset test = new DefaultDataset();
    String line;
    while ((line=br.readLine()) != null) {
      String[] splitLine = line.split("\t");
      if (splitLine.length != 7) continue;
      SparseInstance instance = createInstance(getQueryPojoFromSplitLine(splitLine));
      if(instance == null)
        continue;
      dataset.add(instance);
      // Separate into training and test
      Random random = new Random();
      int next = random.nextInt(100);
      if (next >= 70) {
        test.add(instance);
      }
      else {
        train.add(instance);
      }
    }
    //Train and test datasets
    FileHandler.exportDataset(train,
        new File(exportedTrainDataset));
    LOG.info("Training set size: " + train.size());
    LOG.info("Classindex: " + train.classIndex(null));
    FileHandler.exportDataset(test,
        new File(exportedTestDataset));
    LOG.info("Test set size: " + test.size());
    LOG.info("Classindex: " + test.classIndex(null));
  }

  private static SparseInstance createInstance(QueryPojo queryPojo) throws IOException {
    SparseInstance instance = new SparseInstance();
    QuerySamplingDetails querySamplingDetails = mapper.readValue(queryPojo.getParams(), QuerySamplingDetails.class);
    QuerySamplingDetails.Details details = querySamplingDetails.getDetails();
    int start = details.getStart();
    addDoubleField(instance, "start", start);

    String query = details.getQuery();
    if(query != null) {
      query = query.replace("(", "");
      query = query.replace(")", "");
      query = query.replace("\"", "");
      query = query.replace("+", "");
      String[] queryFields = query.split("AND|OR");
      // Number of fields as a separate field
      addDoubleField(instance, "fieldCount", queryFields.length);
    }
    if(queryPojo.getQtime() == null) {}
    else {
      int qtime = Integer.parseInt(queryPojo.getQtime());
      // --------- for classification --------------
      if(qtime < slowQueryThreshold) {
        instance.setClassValue(new Double(0));
      }
      else {
        instance.setClassValue(new Double(1));
      }
    }
    if(queryPojo.getHits() == null) {}
    else {
      int hits = Integer.parseInt(queryPojo.getHits());
      addDoubleField(instance, "hits", hits);
    }
    addBitmaskBooleanFields(instance, queryPojo.getBitmask());
    return instance;
  }

  private static void addBitmaskBooleanFields(SparseInstance instance, String bitmask) {
    if(bitmask.length() != 7) {
      LOG.error("Invalid bitmask: " + bitmask);
      return;
    }

    addDoubleField(instance, "containsRangeQuery", Integer.parseInt(String.valueOf(bitmask.charAt(0))));
    addDoubleField(instance, "isFacetSearch", Integer.parseInt(String.valueOf(bitmask.charAt(1))));
    addDoubleField(instance, "isPropertyLookup", Integer.parseInt(String.valueOf(bitmask.charAt(2))));
    addDoubleField(instance, "isPropertyHashLookup", Integer.parseInt(String.valueOf(bitmask.charAt(3))));
    addDoubleField(instance, "isCollapsingSearch", Integer.parseInt(String.valueOf(bitmask.charAt(4))));
    addDoubleField(instance, "isGeospatialSearch", Integer.parseInt(String.valueOf(bitmask.charAt(5))));
    addDoubleField(instance, "containsOpenHomes", Integer.parseInt(String.valueOf(bitmask.charAt(6))));
  }

  private static void addDoubleField(SparseInstance instance, String attributeName, int attributeValue) {
    if(attributeIndex.containsKey(attributeName)) {
      int index = attributeIndex.get(attributeName);
      instance.put(index, (double) attributeValue);
    }
    else {
      int index = attributeCount;
      attributeIndex.put(attributeName, attributeCount++);
      instance.put(index, (double) attributeValue);
    }
  }

  private static void addStringField(SparseInstance instance, String attribute) {
    if(attributeIndex.containsKey(attribute)) {
      int index = attributeIndex.get(attribute);
      instance.put(index, (double) 1);
    }
    else {
      int index = attributeCount;
      attributeIndex.put(attribute, attributeCount++);
      instance.put(index, (double) 1);
    }
  }
  private static QueryPojo getQueryPojoFromSplitLine(String[] fields){
    QueryPojo queryPojo = new QueryPojo();
    queryPojo.setParams(fields[3]);
    if (!fields[4].isEmpty()) queryPojo.setQtime(fields[4]);
    if (!fields[5].isEmpty()) queryPojo.setHits(fields[5]);
    queryPojo.setBitmask(fields[6]);
    return queryPojo;
  }


}

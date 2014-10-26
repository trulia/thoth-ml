package com.trulia.thoth;

import com.trulia.thoth.pojo.QueryPojo;
import com.trulia.thoth.pojo.QuerySamplingDetails;
import com.trulia.thoth.util.Utils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.*;
import java.util.ArrayList;
import java.util.Random;

/**
 * User: dbraga - Date: 10/1/14
 */

@Component
public class Model {
  static final int slowQueryThreshold = 50;
  private static final Logger LOG = Logger.getLogger(Model.class);
  static ObjectMapper mapper = new ObjectMapper();
  Random random = new Random();
  private String version;
  @Value("${thoth.merging.dir}")
  private String mergeDirectory;
  @Value("${train.dataset.location}")
  private String exportedTrainDataset;
  @Value("${test.dataset.location}")
  private String exportedTestDataset;

  private static Double[] createInstance(QueryPojo queryPojo) throws IOException {
    ArrayList<Double> instance = new ArrayList<Double>();
    int pos = 0;
    QuerySamplingDetails querySamplingDetails = mapper.readValue(queryPojo.getParams(), QuerySamplingDetails.class);
    QuerySamplingDetails.Details details = querySamplingDetails.getDetails();
    int start = details.getStart();
    instance.add((double) start);

    String query = details.getQuery();
    if(query != null) {
      query = query.replace("(", "");
      query = query.replace(")", "");
      query = query.replace("\"", "");
      query = query.replace("+", "");
      String[] queryFields = query.split("AND|OR");
      // Number of fields as a separate field
      instance.add((double) queryFields.length);
    }
    if(queryPojo.getQtime() == null) {
      // LOG missing qtime
      return null;
    }
    else {
      int qtime = Integer.parseInt(queryPojo.getQtime());
      // --------- for classification --------------
      if(qtime < slowQueryThreshold) {
        instance.add(0.0);
      }
      else {
        instance.add(1.0);
      }
    }
    if(queryPojo.getHits() == null) {
      // Log missing hits
      // How critical is this? Can this ever be missing
    }
    else {
      int hits = Integer.parseInt(queryPojo.getHits());
      instance.add((double) hits);
    }
    addBitmaskBooleanFields(instance, queryPojo.getBitmask());
    return instance.toArray(new Double[instance.size()]);
  }

  private static void addBitmaskBooleanFields(ArrayList<Double> instance, String bitmask) {
    if(bitmask.length() != 7) {
      LOG.error("Invalid bitmask: " + bitmask);
      return;
    }

    instance.add(Double.parseDouble(String.valueOf(bitmask.charAt(0))));
    instance.add(Double.parseDouble(String.valueOf(bitmask.charAt(1))));
    instance.add(Double.parseDouble(String.valueOf(bitmask.charAt(2))));
    instance.add(Double.parseDouble(String.valueOf(bitmask.charAt(3))));
    instance.add(Double.parseDouble(String.valueOf(bitmask.charAt(4))));
    instance.add(Double.parseDouble(String.valueOf(bitmask.charAt(5))));
    instance.add(Double.parseDouble(String.valueOf(bitmask.charAt(6))));
  }

  private static QueryPojo getQueryPojoFromSplitLine(String[] fields){
    QueryPojo queryPojo = new QueryPojo();
    queryPojo.setParams(fields[3]);
    if (!fields[4].isEmpty()) queryPojo.setQtime(fields[4]);
    if (!fields[5].isEmpty()) queryPojo.setHits(fields[5]);
    queryPojo.setBitmask(fields[6]);
    return queryPojo;
  }

  public static void main(String[] args) throws IOException {
    new Model().generateDataSet();
  }

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

  /**
   * Retrieve model version
   * @return String representation of the version
   */
  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
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
   * Generate data set and train model
   * @throws IOException
   */
  public void generateDataSet() throws IOException {
    // Get file that contains Thoth sample data
    BufferedReader br = new BufferedReader(new FileReader(Utils.getThothSampledFileName(mergeDirectory)));
    // Training and test datasets
    ArrayList<Double[]> train = new ArrayList<Double[]>();
    ArrayList<Double[]> test = new ArrayList<Double[]>();

    String line;
    while ((line=br.readLine()) != null) {
      String[] splitLine = line.split("\t");
      if (splitLine.length != 7) continue;
      Double[] instance = createInstance(getQueryPojoFromSplitLine(splitLine));
      if(instance == null)
        continue;

      // Separate into training and test

      int next = random.nextInt(100);
      if (next >= 70) {
        test.add(instance);
      }
      else {
        train.add(instance);
      }
    }

    // Export train and test datasets
    //exportedTrainDataset = "/tmp/trained";
    //exportedTestDataset = "/tmp/tested";


    exportDataset(train, exportedTrainDataset);
    exportDataset(test, exportedTestDataset);
    LOG.info("Training set size: " + train.size());
    LOG.info("Test set size: " + test.size());


  }

  private void exportDataset(ArrayList<Double[]> dataset, String path) throws IOException {
    if(dataset == null) {
      LOG.info("Empty dataset. Nothing to export");
      return;
    }

    BufferedWriter bw = new BufferedWriter(new FileWriter(path));
    for(Double[] example: dataset) {
      if(example.length != 10) {
        // Perform this check?
      }
      StringBuffer sb = new StringBuffer();
      for(Double value: example) {
        sb.append(value + "\t");
      }
      bw.write(sb.toString().trim());
      bw.newLine();
    }
    bw.flush();
  }


}

package com.trulia.thoth.quartz;

import com.trulia.thoth.pojo.QueryPojo;
import com.trulia.thoth.pojo.QuerySamplingDetails;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.*;
import java.util.ArrayList;

/**
 * User: dbraga - Date: 10/13/14
 */
public class Test {

  static final int slowQueryThreshold = 100;
  static ObjectMapper mapper = new ObjectMapper();
  static {
    mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  private static void addBitmaskBooleanFields(ArrayList<Double> instance, String bitmask) {
    if(bitmask.length() != 7) {
      System.out.println("Invalid bitmask: " + bitmask);
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


  public void generateDataSet() throws IOException {
    System.out.println("Generating dataset ...");
    // Get file that contains Thoth sample data
    BufferedReader br = new BufferedReader(new FileReader("/Users/dbraga/dr"));
    // Training and Test datasets
    ArrayList<Double[]> train = new ArrayList<Double[]>();
    //ArrayList<Double[]> test = new ArrayList<Double[]>();

    String line;
    while ((line=br.readLine()) != null) {
      String[] splitLine = line.split("\t");
      if (splitLine.length != 7) continue; //TODO: too specific, need to make it generic
      Double[] instance = null;
      instance = createInstance(getQueryPojoFromSplitLine(splitLine));
      if(instance == null) continue;

      //System.out.println("instance " + ArrayUtils.toString(instance));

      // Separate into training and test
      // Random random = new Random();
      //int next = random.nextInt(100);
      //if (next >= 70) {
        //test.add(instance);
      //}
      //else {
        train.add(instance);
      //}
    }

    //    int positive = 0, negative = 0;
    //    for(int i=0; i<train.size(); i++) {
    //      Double[] row = train.get(i);
    //      Double label = row[0];
    //      if(label != null) {
    //        if(label == 1.0)
    //          positive++;
    //        else if(label == 0.0)
    //          negative++;
    //        else
    //          LOG.info("Invalid class label");
    //      }
    //      else {
    //        LOG.info("Null class label");
    //      }
    //    }
    //
    //    LOG.info("Positive: " + positive + " Negative: " + negative);

    // Export train and test datasets
    exportDataset(train, "/Users/dbraga/dr_trained");
    //exportDataset(test, exportedTestDataset);
    System.out.println("Training set size: " + train.size());
    //System.out.println("Test set size: " + test.size());

  }


  /**
   * Exports dataset to file
   * @param dataset ArrayList of double arrays
   * @param path of the file that needs to be stored
   * @throws java.io.IOException
   */
  private void exportDataset(ArrayList<Double[]> dataset, String path) throws IOException {
    if (dataset == null) {
      System.out.println("Empty dataset. Nothing to export. Skipping ...");
      return;
    }


    BufferedWriter bw = new BufferedWriter(new FileWriter(path));
    for (Double[] example: dataset) {
      if (example.length != 10) { //TODO: too specific, need to make it generic
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
    bw.close();
  }


  private static Double[] createInstance(QueryPojo queryPojo) throws IOException {
    ArrayList<Double> instance = new ArrayList<Double>();
    int pos = 0;
    try {
      QuerySamplingDetails querySamplingDetails = mapper.readValue(queryPojo.getParams(), QuerySamplingDetails.class);
      QuerySamplingDetails.Details details = querySamplingDetails.getDetails();

      if(queryPojo.getQtime() == null) {
        // Handle this differently during prediction
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
      else {
        //        LOG.info(queryPojo.getParams());
        return null;
      }

      //    if(queryPojo.getHits() == null) {
      //      // Log missing hits
      //      // How critical is this? Can this ever be missing
      //    }
      //    else {
      //      int hits = Integer.parseInt(queryPojo.getHits());
      //      instance.add((double) hits);
      //    }
      addBitmaskBooleanFields(instance, queryPojo.getBitmask());
      return instance.toArray(new Double[instance.size()]);
    }
    catch (Exception ignored){
      //      System.out.println("$$$$$$$$$$$ EXCEPTION  "+ queryPojo.getParams());
    }
    return  null;
  }

  public static void main(String[] args) throws IOException {
    new Test().generateDataSet();

  }

}

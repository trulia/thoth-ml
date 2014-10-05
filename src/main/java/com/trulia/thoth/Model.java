package com.trulia.thoth;

import com.trulia.thoth.pojo.QueryPojo;
import com.trulia.thoth.pojo.QuerySamplingDetails;
import com.trulia.thoth.util.Utils;
import net.sf.javaml.core.Dataset;
import net.sf.javaml.core.DefaultDataset;
import net.sf.javaml.core.SparseInstance;
import net.sf.javaml.tools.data.FileHandler;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

/**
 * User: dbraga - Date: 10/1/14
 */

@Component
public class Model {
  private String version;
  static ObjectMapper mapper = new ObjectMapper();
  static HashMap<String, Integer> attributeIndex = new HashMap<String, Integer>();
  static int attributeCount = 0;
  static final int slowQueryThreshold = 50;
  @Value("${thoth.merging.dir}")
  private String mergeDirectory;

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

  public void generateDataSet() throws IOException {
   BufferedReader br = new BufferedReader(new FileReader(Utils.getThothSampledFileName(mergeDirectory)));

    // hostname_s, pool_s, source_s, params_s, qtime_i, hits_i, bitmask_s
    //bw.write("");
    Dataset dataset = new DefaultDataset();
    ArrayList<Integer> qtimes = new ArrayList<Integer>();
    int slowQueries = 0, fastQueries = 0;

    // Training and test datasets
    Dataset train = new DefaultDataset();
    Dataset test = new DefaultDataset();

    String line = new String();
    int emptyCount = 0;
    while ( (line=br.readLine()) != null ) {

      //DenseInstance instance = createInstance(line);
      String[] splitLine = line.split("\t");
      if (splitLine.length != 7) continue;
      SparseInstance instance = createInstance(getQueryPojoFromSplittedLine(splitLine));

      if(instance == null)
        continue;

      dataset.add(instance);

      // Separate into training and test
      Random random = new Random();
      int next = random.nextInt(100);
      if(next >= 70) {
        test.add(instance);
      }
      else {
        train.add(instance);
      }
    }

    //    System.out.println("Empty count: " + emptyCount);
    //
    //    Collections.sort(qtimes);
    //    int size = qtimes.size();
    //    System.out.println("Query time stats: ");
    //    System.out.println("min: " + qtimes.get(0) + " max: " + qtimes.get(size - 1) + " median: " + qtimes.get( size/2 ));
    //
    //    System.out.print("#slow: " + slowQueries + " #fast: " + fastQueries);

    // Optional normalization step
    //    NormalizeMidrange nmr = new NormalizeMidrange(0.5, 1);
    //    nmr.filter(dataset);
    //    nmr.filter(train);
    //    nmr.filter(test);

    // FileHandler.exportDataset(dataset, new File("/Users/pmhatre/thoth-data/preparedDatasetNormalized"));
    //Train and test datasets
    FileHandler.exportDataset(train,
        new File("/Users/dbraga/full_train_50_all_features_noquery3"));
    System.out.println("Training set size: " + train.size());
    System.out.println("Classindex: " + train.classIndex(null));
    FileHandler.exportDataset(test,
        new File("/Users/dbraga/full_test_50_all_features_noquery3"));
    System.out.println("Test set size: " + test.size());
    System.out.println("Classindex: " + test.classIndex(null));

    //for(String key: attributeIndex.keySet()) {
    //  System.out.println(key + ": " + attributeIndex.get(key));
    //}
  }

  private static SparseInstance createInstance(QueryPojo queryPojo) throws IOException {
    SparseInstance instance = new SparseInstance();
    //String[] fields = line.split("\t");
    //if(fields.length != 7) {
    //  return null;
    //}

    //1. hostname_s
    //String hostName = fields[0];
    //addStringField(instance, "hostname_" + hostName);

    // 2. pool_s
    //String pool = fields[1];
    //addStringField(instance, "pool_" + pool);

    // 3. source_s
    //String source = fields[2];
    //addStringField(instance, "source_" + source);

    // 4. param_s

    //String params = fields[3];
    //      System.out.println(params);


    QuerySamplingDetails querySamplingDetails = mapper.readValue(queryPojo.getParams(), QuerySamplingDetails.class);
    QuerySamplingDetails.Details details = querySamplingDetails.getDetails();
    int start = details.getStart();
    //      if(start != 0) {
    //        System.out.println("start: " + start);
    //      }

    addDoubleField(instance, "start", start);

    String query = details.getQuery();
    if(query != null) {
      //        System.out.println("valid query");
      query = query.replace("(", "");
      query = query.replace(")", "");
      query = query.replace("\"", "");
      query = query.replace("+", "");
      String[] queryFields = query.split("AND|OR");
      //        for(String queryField: queryFields) {
      //          String[] pair = queryField.split(":");
      //          if(pair.length  == 2) {
      //            String fieldName = pair[0];
      //            addStringField(instance, "field:" + fieldName);
      //          }
      //          else {
      //            System.out.println("Invalid field key value pair");
      //          }
      //        }

      // Number of fields as a separate field
      addDoubleField(instance, "fieldCount", queryFields.length);
      //        System.out.println("fieldCount: " + queryFields.length);
    }

    //      if(details.isSlowpool()) {
    //        addDoubleField(instance, "slowpool", 1);
    //      }
    //      else {
    //        addDoubleField(instance, "slowpool", 0);
    //      }

    // 5. qtime_i
    // ---------------- add isNumber check -----------------
    if(queryPojo.getQtime() == null) {
      //      emptyCount++;
    }
    else {
      int qtime = Integer.parseInt(queryPojo.getQtime());
      //addDoubleField(instance, "qtime", qtime);
      //      qtimes.add(qtime);
      //      instance.setClassValue(new Double(qtime)); // for regression

      // --------- for classification --------------
      if(qtime < slowQueryThreshold) {
        //          fastQueries++;
        instance.setClassValue(new Double(0));
      }
      else {
        //          slowQueries++;
        instance.setClassValue(new Double(1));
      }
    }


    // 6. hits_i
    if(queryPojo.getHits() == null) {
      //      emptyCount++;
    }
    else {
      int hits = Integer.parseInt(queryPojo.getHits());
      addDoubleField(instance, "hits", hits);
    }

    // 7. bitmask_s
    addBitmaskBooleanFields(instance, queryPojo.getBitmask());

    return instance;
  }

  private static void addBitmaskBooleanFields(SparseInstance instance, String bitmask) {
    if(bitmask.length() != 7) {
      //System.out.println("Invalid bitmask: " + bitmask);
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
  private static QueryPojo getQueryPojoFromSplittedLine(String[] fields){
    QueryPojo queryPojo = new QueryPojo();
    queryPojo.setParams(fields[3]);
    if (!fields[4].isEmpty()) queryPojo.setQtime(fields[4]);
    if (!fields[5].isEmpty()) queryPojo.setHits(fields[5]);
    queryPojo.setBitmask(fields[6]);
    return queryPojo;
  }


}

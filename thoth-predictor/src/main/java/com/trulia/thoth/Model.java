package com.trulia.thoth;

import com.trulia.thoth.trulia.TruliaConverter;
import hex.gbm.GBM;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import water.Boot;
import water.H2O;
import water.Key;
import water.UKV;
import water.api.AUC;
import water.api.AUCData;
import water.fvec.Frame;
import water.fvec.NFSFileVec;
import water.fvec.ParseDataset2;
import water.fvec.Vec;
import water.serial.Model2FileBinarySerializer;

import javax.annotation.PostConstruct;
import java.io.*;
import java.util.ArrayList;
import java.util.Random;

/**
 * User: dbraga - Date: 10/1/14
 */

@Component
public class Model {
  private static final Logger LOG = Logger.getLogger(Model.class);
  private static final String H2O_CLOUD_NAME = "predictorCloud";
  private Random random;
  private String version;
  @Value("${thoth.predictor.slowquery.threshold}")
  private int slowQueryThreshold;
  @Value("${thoth.merging.dir}")
  private String mergeDirectory;
  @Value("${train.dataset.location}")
  private String exportedTrainDataset;
  @Value("${test.dataset.location}")
  private String exportedTestDataset;
  @Value("${model.location}")
  private String modelLocation;


  @PostConstruct
  public void init() {
    random = new Random();
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

  public String getModelLocation() {
    return modelLocation;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  /**
   * Set new version both on disk and in memory
   */
  private void setNewVersion(String version){
    // Write new version to disk
    File f = new File("version");
    PrintWriter pw = null;
    try {
      pw = new PrintWriter(f);
      pw.write(version);
      pw.close();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
    // Set new version in memory
    this.version = version;
  }


  /**
   * Create a newly version using time since epoch
   * @return new version
   */
  private String generateNewVersion(){
    // Fetch new version
    return String.valueOf(System.currentTimeMillis());
  }


  /**
   * Train the model and return the newly model version
   * @return
   */
  public String trainModel() throws Exception {
    String tempVersion = generateNewVersion();
    LOG.info("Training new model... new model will have version(" + tempVersion + ")");
    trainAndStoreModel(tempVersion);
    setNewVersion(tempVersion);
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
   * Export the data sets to disk so a model can be train on those
   * @param testDataSet array list of test instances
   * @param trainDataSet array list of train instances
   * @throws IOException
   */
  private void exportDataSets(ArrayList<Double[]> testDataSet, ArrayList<Double[]> trainDataSet) throws IOException {
    // Export train and test datasets
    exportDataset(testDataSet, exportedTrainDataset);
    exportDataset(trainDataSet, exportedTestDataset);
    LOG.info("Training set size(" + trainDataSet.size()+") exported in " + exportedTrainDataset);
    LOG.info("Test set size(" + testDataSet.size()+") exported in " + exportedTestDataset);
  }



  /**
   * Generate and export train and test data sets
   * @throws java.io.IOException
   */
  public void generateDataSet() throws IOException {
    LOG.info("Generating Data sets ...");
    // Retrieve file that contains Thoth sample data
    BufferedReader br = new BufferedReader(new FileReader(MergeUtils.getMergeFile(mergeDirectory)));

    // Prepare Training and Test data set
    ArrayList<Double[]> train = new ArrayList<Double[]>();
    ArrayList<Double[]> test = new ArrayList<Double[]>();

    String line;
    while ((line=br.readLine()) != null) {
      Double[] instance = Instance.create(TruliaConverter.tsvToQueryPojo(line), slowQueryThreshold, false);
      if (instance == null) continue; // invalid instance, skipping
      LOG.debug("Instance: " + ArrayUtils.toString(instance));
      // Separate into training and test
      if (random.nextInt(100) >= 70) {
        test.add(instance);
      }
      else {
        train.add(instance);
      }
    }
    exportDataSets(test,train);
  }



  /**
   * Exports dataSet to file
   * @param dataSet ArrayList of double arrays
   * @param path of the file that needs to be stored
   * @throws java.io.IOException
   */
  private void exportDataset(ArrayList<Double[]> dataSet, String path) throws IOException {
    if (dataSet == null) {
      LOG.warn("Empty dataSet. Nothing to export. Skipping ...");
      return;
    }
    LOG.debug("Trained set: " + exportedTrainDataset);
    BufferedWriter bw = new BufferedWriter(new FileWriter(path));
    for (Double[] example: dataSet) {
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

  /**
   * Boot the H2o cloud, train a new model and store it to disk
   * @param modelVersion unique version for the new model
   * @throws Exception
   */
  public void trainAndStoreModel(String modelVersion) throws Exception {
        System.out.println("Initialization of the H2oCloud ...");
        Boot.main(Model.class, new String[]{exportedTrainDataset, exportedTestDataset, modelLocation, modelVersion});
  }

  /**
   * Method helper to spawn the H2o Cloud
   * @param args
   * @throws java.io.IOException
   */
  public static void main(String[] args) throws IOException {
    H2O.main(new String[]{"-name", H2O_CLOUD_NAME, "-md5skip", "-Xmx4g"});
    H2O.waitForCloudSize(1);
    System.out.println("H2oCloud("+H2O_CLOUD_NAME+") initialized.");

    File trainedDataSetFile = new File(args[0]);
    Key fkey1 = NFSFileVec.make(trainedDataSetFile);
    Key dest1 = Key.make("thoth-train.hex");

    File file2 = new File(args[1]);
    Key fkey2 = NFSFileVec.make(file2);
    Key dest2 = Key.make("thoth-test.hex");
    Frame ftest = ParseDataset2.parse(dest2, new Key[]{fkey2});

    GBM gbm = new GBM();
    gbm.source = ParseDataset2.parse(dest1, new Key[]{fkey1});
    gbm.response = new PrepData() { @Override
                                    Vec prep(Frame fr) { return fr.vecs()[0]; } }.prep(gbm.source);
    gbm.ntrees = 1000;
    gbm.balance_classes = false;
    gbm.learn_rate = 0.1f;
    gbm.min_rows = 10;
    gbm.nbins = 20;
    gbm.cols =  new int[] {1,2,3,4,5,6};
    gbm.validation = ftest;
    gbm.invoke();
    GBM.GBMModel model = UKV.get(gbm.dest());

    // Get threshold
    Frame fpreds = gbm.score(ftest);
    AUC auc = new AUC();
    auc.actual = ftest;
    auc.vactual = ftest.vecs()[ftest.find("C1")];
    auc.predict = fpreds;
    auc.vpredict = fpreds.vecs()[2];
    auc.invoke();

    AUCData aucData = auc.data();
    aucData.threshold_criterion = AUC.ThresholdCriterion.maximum_F1;
    double threshold = aucData.threshold();
    // Model serialization
    File modelFile = new File(args[2] + "/gbm_model_v" + args[3]);
    new Model2FileBinarySerializer().save(model, modelFile);
    // Writing threshold to disk
    File thresholdFile = new File(args[2] + "/gbm_model_threshold_v" + args[3]);
    PrintWriter pw = new PrintWriter(new FileWriter(thresholdFile));
    pw.write(String.valueOf(threshold));
    pw.close();
  }

  private abstract static class PrepData { abstract Vec prep(Frame fr); }


}

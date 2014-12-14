package com.trulia.thoth.prediction;

import hex.gbm.GBM;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import water.Boot;
import water.H2O;
import water.serial.Model2FileBinarySerializer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.util.ArrayList;


/**
 * User: dbraga - Date: 9/16/14
 *
 * Train the classifier with a specific model
 */

public class ModelTrainer{

  private static Logger LOG = Logger.getLogger(ModelTrainer.class);
  private ThothModel thothModel;
  private String thothPredictorUri;
  private boolean isLocalModelValid = false;
  private static final String INVALID_VERSION = "-1";
  private static final String INVALIDATE_MODEL_VERSION = "-500";
  private String modelLocalVersion = INVALID_VERSION;
  private String modelRemoteVersion = INVALID_VERSION;
  private double threshold;



  /**
   * As for init, fetch local version of the model - if any version is present -
   * If no version is currently present retrieve and use the new model
   */
  public void init() throws Exception {
    thothModel = new ThothModel();

    if (INVALID_VERSION.equals(thothModel.fetchLocalVersion())) {
      isLocalModelValid = false;
    } else {
      this.isLocalModelValid =  retrieveAndSwapModel();
      LOG.info("Local model " + (isLocalModelValid? "valid ...":"invalid ..."));
    }
  }

  /**
   * Check the remote for the existance of a new model
   * @return
   */
  public boolean isNewModelAvailable(){
    if (!INVALID_VERSION.equals(modelRemoteVersion)){
      return Long.parseLong(modelRemoteVersion) > Long.parseLong(modelLocalVersion);
    }
    else return false;
  }


  /**
   * Failsafe for the model, if the model has been invalidated or it's valid to be used
   * @return true if valid, false if not
   */
  public boolean shouldUseModel(){
    LOG.debug("Should use model " + this.isLocalModelValid + "  " + isLocalModelValid);
    return isLocalModelValid;
  }

  public void fetchModelFromRemote(String remoteVersion) throws IOException {
    org.apache.commons.io.FileUtils.copyURLToFile(new URL(thothPredictorUri + "/models/" + remoteVersion),
        new File("downloaded_"+remoteVersion));
    LOG.info("Done downloading model from remote");
  }

  public double fetchThresholdFromRemote(String remoteVersion) throws IOException {
    org.apache.commons.io.FileUtils.copyURLToFile(new URL(thothPredictorUri + "/threshold/" + remoteVersion),
        new File("downloaded_threshold_"+remoteVersion));

    BufferedReader br = new BufferedReader(new FileReader(new File("downloaded_threshold_"+remoteVersion)));
    double threshold = Double.parseDouble(br.readLine());
    LOG.info("Done downloading model threshold from remote("+threshold+")");
    return threshold;
  }

  public void loadModel(String remoteVersion) throws Exception {
    Boot.main(ModelTrainer.class, new String[]{remoteVersion});
  }
  public static void main(String[] args) throws IOException {
    String cloudName = InetAddress.getLocalHost().getHostName();
    H2O.main(new String[]{"-name", cloudName, "-md5skip"});
    H2O.waitForCloudSize(1);
    // Fix logging
    Logger rootLogger = Logger.getRootLogger();
    rootLogger.setLevel(Level.INFO);
    PatternLayout layout = new PatternLayout("%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n");
    //Add console appender to root logger
    rootLogger.addAppender(new ConsoleAppender(layout));

    LOG.info("H2oCloud("+cloudName+") initialized.");
    String modelFileName = "downloaded_"+args[0];
    File modelFile = new File(modelFileName);
    if (modelFile == null) {
      LOG.error("Model file("+"downloaded_"+args[0]+") is null");
    }
    else {
      LOG.info("Model file ("+"downloaded_"+args[0]+") was found");
    }

    // Get keys of current models
    ArrayList<GBM.GBMModel> currentModels = new ArrayList<GBM.GBMModel>();
    for (String k:  H2O.KeySnapshot.globalSnapshot().fetchAll(water.Model.class).keySet()){
      currentModels.add((GBM.GBMModel) H2O.KeySnapshot.globalSnapshot().fetchAll(water.Model.class).get(k));
    }
    // Load model
    GBM.GBMModel model1 = (GBM.GBMModel) new Model2FileBinarySerializer().load(modelFile);
    LOG.info("Model("+model1._key+") loaded");
    LOG.info("Model to purge: " + currentModels.size() );
    // Remove old models
    for (GBM.GBMModel model: currentModels){
      LOG.info("Purging old model("+model._key+")..");
      model.delete();
    }
  }

  /**
   * Fetch model from remote, swap it with the current model
   * For now, since we don't have a remote yet - we will retrain the model locally
   */
  public boolean retrieveAndSwapModel() throws Exception {
    boolean success = false;

    try {
      String remoteVersion = thothModel.fetchRemoteVersion(thothPredictorUri + "/?action=getCurrentModelVersion");
      LOG.info("Fetching new model version from remote ... ");
      fetchModelFromRemote(remoteVersion);
      this.threshold = fetchThresholdFromRemote(remoteVersion);
      LOG.info("Loading model ...");
      loadModel(remoteVersion);

      success = true;
    } catch (Exception e) {
      LOG.error(e);
    }
    return success;
  }


  public double getThreshold() {
    return threshold;
  }

  /**
   * Refresh pointer to local version and to remote version of the model(s)
   */
  public void refreshVersionPointers(){
    String locVersion = thothModel.fetchLocalVersion();
    if (!modelLocalVersion.equals(locVersion)){
      modelLocalVersion = locVersion;
    }
    String remVersion = thothModel.fetchRemoteVersion(thothPredictorUri + "/?action=getCurrentModelVersion");
    if (!modelRemoteVersion.equals(remVersion)){
      modelRemoteVersion = remVersion;
    }
  }

  /**
   * Refresh model time to time, exercise the right to put the current model in invalid state and skip the model
   */
  public void refresh() throws Exception {
    refreshVersionPointers();
    LOG.info("Refreshing model(" + modelLocalVersion + ")");
    // Check if we should stop using the model first,
    if (INVALIDATE_MODEL_VERSION.equals(modelRemoteVersion)){
      LOG.warn("Received an 'invalidate' model signal. Will stop using Thoth model until new model is available");
      this.isLocalModelValid = false;
    }
    else if (isNewModelAvailable()){
      LOG.info("New model is available("+ modelRemoteVersion +"), refreshing ...");
      retrieveAndSwapModel();
      LOG.info("Done refreshing ...");
      // Update local version to reflect the new remote version
      thothModel.setNewVersion(modelRemoteVersion);
      LOG.info("Updated thoth model to new version("+modelLocalVersion+")");
      this.isLocalModelValid = true;
    } else {
      LOG.info("No need to refresh model. No new model available ...");
    }
  }

  public void setThothPredictorUri(String thothPredictorUri) {
    this.thothPredictorUri = thothPredictorUri;
  }


}
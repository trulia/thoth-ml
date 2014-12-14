package com.trulia.thoth.prediction;

import org.apache.log4j.Logger;
import water.Boot;
import water.H2O;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetAddress;

/**
 * User: dbraga - Date: 12/7/14
 */
/**
 * User: dbraga - Date: 10/21/14
 */

public class Cloud {

  private static Logger LOG = Logger.getLogger(Cloud.class);


  public void init() throws Exception {
    LOG.info("Initialization of the H2oCloud ...");
    Boot.main(Cloud.class, new String[]{});
  }


  public static void main(String[] args) throws IOException {
    // Setting the cloud name to the hostname, so each hostname has its own 1 node cloud without causing naming conflicts
    String cloudName = InetAddress.getLocalHost().getHostName();
    H2O.main(new String[]{"-name", cloudName, "-md5skip"});
    H2O.waitForCloudSize(1);
    LOG.info("H2oCloud initialized...");
  }


  public static float predict(double[] data) throws Exception {
    Class oldClass = Boot._init.loadClass("hex.gbm.PredictionUtils");
    Object utils = oldClass.newInstance();
    Method method = oldClass.getDeclaredMethod("predict", double[].class) ;

    float[] pred = (float[]) method.invoke (utils, data);
    for (float f: pred){
      LOG.debug("Float: " + f);
    }
    return pred[2];
  }



}
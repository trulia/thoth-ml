package com.trulia.thoth.demo;

import com.trulia.thoth.prediction.Cloud;
import com.trulia.thoth.prediction.ModelTrainer;
import com.trulia.thoth.trulia.TruliaConverter;
import com.trulia.thoth.trulia.TruliaInstance;
import org.apache.commons.lang.ArrayUtils;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * User: dbraga - Date: 12/13/14
 */
public class PredictionDemo {
  // Threshold that determines if a request should be considered slow or fast - in ms
  private static final int SLOW_QUERY_THRESHOLD = 100;
  // URI of the thoth predictor
  private static final String THOTH_PREDICTOR_URL ="http://thoth-predictor-url:123456";
  private static  ObjectMapper mapper;

  public static void main(String[] args) throws Exception {
    mapper = new ObjectMapper();
    // Solr request that needs to be predicted
    String solrRequest = "q=*:*";
    // Initialize the model trainer
    ModelTrainer modelTrainer = new ModelTrainer();
    modelTrainer.setThothPredictorUri(THOTH_PREDICTOR_URL);
    modelTrainer.init();
    modelTrainer.refresh();
    // Fetch the current model threshold
    float threshold = (float) modelTrainer.getThreshold();
    System.out.println("Model Threshold: " + threshold);
    // Create the instance from the solr request
    double[] instance = ArrayUtils.toPrimitive(TruliaInstance.create(TruliaConverter.solrQueryToQueryPojo(solrRequest, null, null, mapper), SLOW_QUERY_THRESHOLD, true));
    // Get the prediction
    float prediction = Cloud.predict(instance);
    System.out.println("Query Prediction: " + prediction);
    // Determine if slow query or fast query
    boolean isSlowQuery = (prediction < threshold)? false : true;
    System.out.println("Query is predicted to be " + (isSlowQuery? "slow":"fast"));
  }
}

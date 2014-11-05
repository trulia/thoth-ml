package com.trulia.thoth.predictor;

import com.trulia.thoth.Model;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * User: dbraga - Date: 10/1/14
 */
@Controller
@RequestMapping(value = "/")
  public class PredictorController {

  @Autowired
  ModelHealth modelHealth;
  @Autowired
  private Model model;

  //TODO: To remove ASAP  - BEST-1377
  @Autowired @Qualifier("drStaticModelHealth")
  private StaticModelHealth drStaticModelHealth;
  @Autowired @Qualifier("googleStaticModelHealth")
  private StaticModelHealth googleStaticModelHealth;
  @Autowired @Qualifier("userStaticModelHealth")
  private StaticModelHealth userStaticModelHealth;
  @Autowired @Qualifier("mobileStaticModelHealth")
  private StaticModelHealth mobileStaticModelHealth;


  @Value("${thoth.predictor.model.health.invalid.score}")
  private String PREDICTOR_MODEL_HEALTH_INVALID_SCORE;

  private static final String RETRIEVE_VERSION = "getCurrentModelVersion";
  private static final String INVALIDATE_MODEL = "invalidateModel";
  // Model health score
  private static final String GET_MODEL_HEALTH_SCORE = "getModelHealthScore";

  //TODO: To remove ASAP  - BEST-1377
  private static final String GET_USER_STATIC_MODEL_HEALTH = "getUserScore";
  private static final String GET_MOBILE_STATIC_MODEL_HEALTH = "getMobileScore";
  private static final String GET_DR_STATIC_MODEL_HEALTH = "getDrScore";
  private static final String GET_GOOGLE_STATIC_MODEL_HEALTH = "getGoogleScore";


  private static final String SET_INVALID_MODEL_HEALTH_SCORE = "setInvalidModelHealthScore";
  private static final String RESET_MODEL_HEALTH_SCORE = "resetModelHealthScore";

  private static final String TRAIN_MODEL_ACTION = "trainModel";
  private static final String INVALIDATE_MODEL_VERSION = "-500";

  //TODO: To remove ASAP  - BEST-1377
  private String generateStaticModelHealthJson(StaticModelHealth staticModelHealth){

   return  "{\n" +
        "    \"count\": "+staticModelHealth.getSampleCount()+",\n" +
        "    \"errors\": "+staticModelHealth.getPredictionErrors()+",\n" +
        "    \"falsePositive\": "+staticModelHealth.getFalsePositive()+",\n" +
        "    \"falseNegative\": "+staticModelHealth.getFalseNegative()+",\n" +
        "    \"truePositive\": "+staticModelHealth.getTruePositive()+",\n" +
        "    \"trueNegative\": "+staticModelHealth.getTrueNegative()+",\n" +
       //"    \"f1\": "+staticModelHealth.getF1()+"\n" +
       "    \"avgPerClassError\": "+staticModelHealth.getAvgPerClassError()+"\n" +
        "}";
  }


  @RequestMapping(method = RequestMethod.GET, params = {"action"})
  public ResponseEntity<String> getAction(@RequestParam(value = "action") String action) throws Exception {


  if (RETRIEVE_VERSION.equals(action)){
    return new ResponseEntity<String>(model.getVersion(), HttpStatus.OK);
  }
  else if (TRAIN_MODEL_ACTION.equals(action)){
    model.generateDataSet();
    model.trainModel();
    drStaticModelHealth.resetCounters();
    googleStaticModelHealth.resetCounters();
    mobileStaticModelHealth.resetCounters();
    userStaticModelHealth.resetCounters();
    modelHealth.setHealthScore(0.0f);
    return new ResponseEntity<String>("", HttpStatus.OK);
  }
  else if (INVALIDATE_MODEL.equals(action)){
    model.setVersion(INVALIDATE_MODEL_VERSION);
    return new ResponseEntity<String>(model.getVersion(), HttpStatus.OK);
  }
  else if (GET_MODEL_HEALTH_SCORE.equals(action)){
    return new ResponseEntity<String>(String.valueOf(modelHealth.getHealthScore()), HttpStatus.OK);
  }

  //TODO: To remove ASAP  - BEST-1377
  else if (GET_USER_STATIC_MODEL_HEALTH.equals(action)){
    return new ResponseEntity<String>(generateStaticModelHealthJson(userStaticModelHealth), HttpStatus.OK);
  }
  else if (GET_GOOGLE_STATIC_MODEL_HEALTH.equals(action)){
    return new ResponseEntity<String>(generateStaticModelHealthJson(googleStaticModelHealth), HttpStatus.OK);
  }
  else if (GET_DR_STATIC_MODEL_HEALTH.equals(action)){
    return new ResponseEntity<String>(generateStaticModelHealthJson(drStaticModelHealth), HttpStatus.OK);
  }
  else if (GET_MOBILE_STATIC_MODEL_HEALTH.equals(action)){
    return new ResponseEntity<String>(generateStaticModelHealthJson(mobileStaticModelHealth), HttpStatus.OK);
  }


  else if (SET_INVALID_MODEL_HEALTH_SCORE.equals(action)){
    modelHealth.setHealthScore(Float.parseFloat(PREDICTOR_MODEL_HEALTH_INVALID_SCORE));
    return new ResponseEntity<String>("OK", HttpStatus.OK);
  }
  else if (RESET_MODEL_HEALTH_SCORE.equals(action)){
    modelHealth.setHealthScore(0.0f);
    return new ResponseEntity<String>("OK", HttpStatus.OK);
  }
   return  null;
  }


  @RequestMapping(value = "/models/{model_version}", method = RequestMethod.GET)
  public void getFile(
      @PathVariable("model_version") String version,
      HttpServletResponse response) {
      String fileName = model.getModelLocation() + "/gbm_model_v" + version;
    try {
      // get your file as InputStream
      InputStream is = new FileInputStream(new File(fileName));
      // copy it to response's OutputStream
      org.apache.commons.io.IOUtils.copy(is, response.getOutputStream());
      response.flushBuffer();
    } catch (IOException ex) {
      System.out.println(String.format("Error writing file to output stream. Filename was '{}'", fileName, ex));
      throw new RuntimeException("IOError writing file to output stream");
    }

  }

}

package com.trulia.thoth.predictor;

import com.trulia.thoth.Model;
import org.springframework.beans.factory.annotation.Autowired;
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

  @Value("${thoth.predictor.model.health.invalid.score}")
  private String PREDICTOR_MODEL_HEALTH_INVALID_SCORE;

  private static final String RETRIEVE_VERSION = "getCurrentModelVersion";
  private static final String INVALIDATE_MODEL = "invalidateModel";
  // Model health score
  private static final String GET_MODEL_HEALTH_SCORE = "getModelHealthScore";

  // Check for model health score overflows
  private static final String CHECK_MODEL_HEALTH_OVERFLOW = "checkModelHealthOverflow";
  private static final String RESET_MODEL_HEALTH_SCORE = "resetModelHealthScore";

  private static final String TRAIN_MODEL_ACTION = "trainModel";
  private static final String INVALIDATE_MODEL_VERSION = "-500";

  @RequestMapping(method = RequestMethod.GET, params = {"action"})
  public ResponseEntity<String> getAction(@RequestParam(value = "action") String action) throws Exception {


  if (RETRIEVE_VERSION.equals(action)){
    return new ResponseEntity<String>(model.getVersion(), HttpStatus.OK);
  }
  else if (TRAIN_MODEL_ACTION.equals(action)){
    model.generateDataSet();
    model.trainModel();
    modelHealth.resetCounters();
    return new ResponseEntity<String>("", HttpStatus.OK);
  }
  else if (INVALIDATE_MODEL.equals(action)){
    model.setVersion(INVALIDATE_MODEL_VERSION);
    return new ResponseEntity<String>(model.getVersion(), HttpStatus.OK);
  }

  else if (GET_MODEL_HEALTH_SCORE.equals(action)){
    return new ResponseEntity<String>(String.valueOf(modelHealth.getAvgPerClassError()), HttpStatus.OK);
  }

  else if (RESET_MODEL_HEALTH_SCORE.equals(action)){
    modelHealth.resetCounters();
    return new ResponseEntity<String>("OK", HttpStatus.OK);
  }
  else if (CHECK_MODEL_HEALTH_OVERFLOW.equals(action)){
    return new ResponseEntity<String>(modelHealth.checkCountOverflow()? "overflowed" : "not overflowed", HttpStatus.OK);
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

  @RequestMapping(value = "/threshold/{model_version}", method = RequestMethod.GET)
  public void getThreshold(
      @PathVariable("model_version") String version,
      HttpServletResponse response) {

    String fileName = model.getModelLocation() + "/gbm_model_threshold_v" + version;
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

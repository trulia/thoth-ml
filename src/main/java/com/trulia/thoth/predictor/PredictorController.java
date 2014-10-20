package com.trulia.thoth.predictor;

import com.trulia.thoth.Model;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import java.io.IOException;

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
  private static final String SET_INVALID_MODEL_HEALTH_SCORE = "setInvalidModelHealthScore";
  private static final String RESET_MODEL_HEALTH_SCORE = "resetModelHealthScore";

  private static final String TRAIN_MODEL_ACTION = "trainModel";
  private static final String INVALIDATE_MODEL_VERSION = "-500";

  @RequestMapping(method = RequestMethod.GET, params = {"action"})
  public ResponseEntity<String> getAction(@RequestParam(value = "action") String action) throws IOException {


  if (RETRIEVE_VERSION.equals(action)){
    return new ResponseEntity<String>(model.getVersion(), HttpStatus.OK);
  }
  else if (TRAIN_MODEL_ACTION.equals(action)){
    // TODO: collapse those two methods
    //model.generateDataSet();
    System.out.println("Training model");
    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    modelHealth.setHealthScore(0.0f);
    return new ResponseEntity<String>(model.trainModel(), HttpStatus.OK);
  }
  else if (INVALIDATE_MODEL.equals(action)){
    model.setVersion(INVALIDATE_MODEL_VERSION);
    return new ResponseEntity<String>(model.getVersion(), HttpStatus.OK);
  }
  else if (GET_MODEL_HEALTH_SCORE.equals(action)){
    return new ResponseEntity<String>(String.valueOf(modelHealth.getHealthScore()), HttpStatus.OK);
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

}

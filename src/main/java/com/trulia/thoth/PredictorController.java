package com.trulia.thoth;

import org.springframework.beans.factory.annotation.Autowired;
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

  private static final String RETRIEVE_VERSION = "getCurrentModelVersion";
  private static final String INVALIDATE_MODEL = "invalidateModel";
  private static final String GET_MODEL_HEALTH = "getModelHealth";
  private static final String SET_MODEL_HEALTH = "setModelHealth";
  private static final String TRAIN_MODEL_ACTION = "trainModel";
  private static final String INVALIDATE_MODEL_VERSION = "-500";

  @Autowired
  private Model model;

  @RequestMapping(method = RequestMethod.GET, params = {"action"})
  public ResponseEntity<String> getAction(@RequestParam(value = "action") String action) throws IOException {


  if (RETRIEVE_VERSION.equals(action)){
    return new ResponseEntity<String>(model.getVersion(), HttpStatus.OK);
  }
  else if (TRAIN_MODEL_ACTION.equals(action)){
    // TODO: collapse those two methods
    model.generateDataSet();
    return new ResponseEntity<String>(model.trainModel(), HttpStatus.OK);
  }
  else if (INVALIDATE_MODEL.equals(action)){
    model.setVersion(INVALIDATE_MODEL_VERSION);
    return new ResponseEntity<String>(model.getVersion(), HttpStatus.OK);
  }
  else if (GET_MODEL_HEALTH.equals(action)){
    return new ResponseEntity<String>(String.valueOf(modelHealth.getScore()), HttpStatus.OK);
  }
  else if (SET_MODEL_HEALTH.equals(action)){   //TODO: remove this
    modelHealth.setScore(0.4f);
    return new ResponseEntity<String>("", HttpStatus.OK);
  }
   return  null;
  }

}

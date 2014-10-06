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

  private static final String RETRIEVE_VERSION = "getCurrentModelVersion";
  private static final String INVALIDATE_MODEL = "invalidateModel";
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

   return  null;
  }

}

package com.trulia.thoth;

/**
 * User: dbraga - Date: 10/1/14
 */

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

/**
 * User: dbraga - Date: 10/1/14
 */
@Controller
@RequestMapping(value = "/predict")
public class PredictorController {

  @RequestMapping(method = RequestMethod.GET)
  public ResponseEntity<String> getAction(){
    System.out.println("test");
    return new ResponseEntity<String>("test", HttpStatus.ACCEPTED);
  }

}

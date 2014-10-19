package com.trulia.thoth;

import org.springframework.stereotype.Component;

/**
 * User: dbraga - Date: 10/16/14
 */
@Component
public class ModelHealth {
  private final int MAX_NUM_IMPORTANT_SAMPLES = 10000;
  private float score = 0.0f;
  private int count = 0;

  public float getScore() {
    return score;
  }

  public void setScore(float score){
    this.score = score;
  }

  public void incrementCount(){
    count = Math.min(count+1, MAX_NUM_IMPORTANT_SAMPLES);
  }

  public void computeScore(int error){
    score = score * (1.0f*(count-1) )/ count + (1.0f * error)/(1.0f * count) ;
  }

}

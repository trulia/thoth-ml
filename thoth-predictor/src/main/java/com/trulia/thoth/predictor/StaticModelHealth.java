package com.trulia.thoth.predictor;

import org.springframework.stereotype.Component;

/**
 * User: dbraga - Date: 10/16/14
 */
@Component
public class StaticModelHealth {
  private int sampleCount;
  private int falsePositive;
  private int falseNegative;

  private int truePositive;
  private int trueNegative;

  public float getAvgPerClassError() {
    float score = ((1.0f* falseNegative)/(falseNegative + truePositive) + (1.0f * falsePositive)/(falsePositive+trueNegative))/2;
    if (Float.isNaN(score)) return 0.0f;
    else return score;
  }

  public void incrementTruePositive(){
    truePositive++;
  }

  public void incrementTrueNegative(){
    trueNegative++;
  }


  public void incrementFalsePositive(){
    falsePositive++;
  }

  public void incrementFalseNegative(){
    falseNegative++;
  }

  public int getFalsePositive() {
    return falsePositive;
  }

  public int getFalseNegative() {
    return falseNegative;
  }

  private int predictionErrors;

  public int getSampleCount() {
    return sampleCount;
  }

  public void incrementSampleCount(){
    sampleCount ++ ;
  }

  public int getPredictionErrors() {
    return predictionErrors;
  }

  public void incrementPredictionErrors(){
    predictionErrors++ ;
  }

  public int getTruePositive() {
    return truePositive;
  }

  public int getTrueNegative() {
    return trueNegative;
  }

  //public float getF1() {
  //  return ((2.0f * truePositive)/(2*truePositive + falseNegative + falsePositive));
  //}

  public void resetCounters(){
    sampleCount = 0;
    predictionErrors = 0;
    falsePositive = 0;
    falseNegative = 0 ;
    truePositive = 0;
    trueNegative = 0;
  }


}

package com.trulia.thoth.predictor;

import org.springframework.stereotype.Component;

/**
 * User: dbraga - Date: 10/16/14
 */
@Component
public class ModelHealth {
  private int falsePositive;
  private int falseNegative;
  private int truePositive;
  private int trueNegative;
  private static final int MAX_SAMPLE_COUNT = Integer.MAX_VALUE / 2;

  public float getAvgPerClassError() {
    float score = ((1.0f* falseNegative)/(falseNegative + truePositive) + (1.0f * falsePositive)/(falsePositive+trueNegative))/2;
    if (Float.isNaN(score)) return 0.0f;
    else return score;
  }

  /**
   * Reset all the counters
   */
  public void resetCounters(){
    falsePositive = 0;
    falseNegative = 0 ;
    truePositive = 0;
    trueNegative = 0;
  }

  /**
   * Check if the counters are overflowing over MAX_SAMPLE_COUNT, if so they get divided to keep an accurate amount of counts
   * This method can be called periodically depending of the speed on how fast the counts gets incremented
   * @return true if they overflowed , false if not
   */
  public boolean checkCountOverflow() {
    if(truePositive >= MAX_SAMPLE_COUNT || trueNegative >= MAX_SAMPLE_COUNT || falsePositive >= MAX_SAMPLE_COUNT
        || falseNegative >= MAX_SAMPLE_COUNT) {
      truePositive /= 2;
      trueNegative /= 2;
      falsePositive /= 2;
      falseNegative /= 2;
      return true;
    }
    return false;
  }


}

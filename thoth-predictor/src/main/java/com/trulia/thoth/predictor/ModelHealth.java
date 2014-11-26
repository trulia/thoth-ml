package com.trulia.thoth.predictor;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * User: dbraga - Date: 10/16/14
 */
@Component
public class ModelHealth {
  @Value("${thoth.predictor.max.number.samples}")
  private int MAX_NUM_IMPORTANT_SAMPLES;

  /**
   * Health score of the model, the more the health score is low - the more the model is healthy
   */
  private float healthScore = 0.0f;
  /**
   * Number of samples used to test model health
   */
  private int sample_count = 0;

  public float getHealthScore() {
    return healthScore;
  }

  public void setHealthScore(float healthScore){
    this.healthScore = healthScore;
  }

  public void computeScore(int error){
    // Increment the sample count, but keep the number of samples between [1, MAX_NUM_IMPORTANT_SAMPLES]
    sample_count = Math.min(sample_count +1, MAX_NUM_IMPORTANT_SAMPLES);
    // Calculate the new health score
    healthScore = healthScore * (1.0f*(sample_count -1) )/ sample_count + (1.0f * error)/(1.0f * sample_count) ;
  }

}

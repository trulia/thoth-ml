package hex.gbm;

import org.apache.log4j.Logger;
import water.H2O;

/**
 * User: dbraga - Date: 10/27/14
 */

public class PredictionUtils {
  private static Logger LOG = Logger.getLogger(PredictionUtils.class);

  public float[] predict(double[] data){
    float[] preds = new float[3];
    // Get current model key
    GBM.GBMModel model = (GBM.GBMModel) H2O.KeySnapshot.globalSnapshot().fetchAll(water.Model.class).entrySet().iterator().next().getValue();
    LOG.debug("Model key: " + model._key);
    model.score0(data,preds);
    return preds;
  }

}

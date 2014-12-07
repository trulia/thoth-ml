package com.trulia.thoth;


import com.trulia.thoth.pojo.QueryPojo;
import com.trulia.thoth.pojo.QuerySamplingDetails;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;

/**
 * User: dbraga - Date: 12/6/14
 */
public class Instance {

  static ObjectMapper mapper = new ObjectMapper();

  static {
    mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }


  /**
   * Create an instance
   *
   * @param queryPojo query pojo
   * @param slowQueryThreshold threshold decided to define a query fast/slow
   * @return instance
   * @throws java.io.IOException
   */
  public static Double[] create(QueryPojo queryPojo, int slowQueryThreshold, boolean isPredictionTime) throws IOException {
    ArrayList<Double> instance = new ArrayList<Double>();
    try {
      QuerySamplingDetails querySamplingDetails = mapper.readValue(queryPojo.getParams(), QuerySamplingDetails.class);
      QuerySamplingDetails.Details details = querySamplingDetails.getDetails();

      if (!isPredictionTime) {
        // Creating instance for Model Training
        if (queryPojo.getQtime() == null) {
          return null;
        } else {
          int qtime = Integer.parseInt(queryPojo.getQtime());
          // --------- for classification --------------
          if (qtime < slowQueryThreshold) {
            instance.add(0.0);
          } else {
            instance.add(1.0);
          }
        }
      }

      int start = details.getStart();
      instance.add((double) start);

      String query = details.getQuery();
      if (query != null) {
        query = query.replace("(", "");
        query = query.replace(")", "");
        query = query.replace("\"", "");
        query = query.replace("+", "");
        String[] queryFields = query.split("AND|OR");
        // Number of fields as a separate field
        instance.add((double) queryFields.length);
      } else {
        return null;
      }
      addBitmaskBooleanFields(instance, queryPojo.getBitmask());

      return instance.toArray(new Double[instance.size()]);
    } catch (Exception ignored) {
    }
    return null;
  }


  /**
   * Add bitmask boolean fields to instance
   * @param instance
   * @param bitmask
   */
  private static void addBitmaskBooleanFields(ArrayList<Double> instance, String bitmask) {
    if (bitmask.length() != 4) {
      System.out.println("Invalid bitmask: " + bitmask);
      return;
    }

    instance.add(Double.parseDouble(String.valueOf(bitmask.charAt(0))));
    instance.add(Double.parseDouble(String.valueOf(bitmask.charAt(1))));
    instance.add(Double.parseDouble(String.valueOf(bitmask.charAt(2))));
    instance.add(Double.parseDouble(String.valueOf(bitmask.charAt(3))));
  }




}

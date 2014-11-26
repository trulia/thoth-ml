package com.trulia.thoth;

import com.trulia.thoth.pojo.QueryPojo;
import com.trulia.thoth.pojo.QuerySamplingDetails;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;

/**
 * User: dbraga - Date: 11/25/14
 */
public class Instance {

  static ObjectMapper mapper = new ObjectMapper();
  static {
    mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  public static Double[] create(QueryPojo queryPojo, int slowQueryThreshold) throws IOException {
    ArrayList<Double> instance = new ArrayList<Double>();
    int pos = 0;
    try {
      QuerySamplingDetails querySamplingDetails = mapper.readValue(queryPojo.getParams(), QuerySamplingDetails.class);
      QuerySamplingDetails.Details details = querySamplingDetails.getDetails();

      if(queryPojo.getQtime() == null) {
        // Handle this differently during prediction
        return null;
      }
      else {
        int qtime = Integer.parseInt(queryPojo.getQtime());
        // --------- for classification --------------
        if(qtime < slowQueryThreshold) {
          instance.add(0.0);
        }
        else {
          instance.add(1.0);
        }
      }

      int start = details.getStart();
      instance.add((double) start);

      String query = details.getQuery();
      if(query != null) {
        query = query.replace("(", "");
        query = query.replace(")", "");
        query = query.replace("\"", "");
        query = query.replace("+", "");
        String[] queryFields = query.split("AND|OR");
        // Number of fields as a separate field
        instance.add((double) queryFields.length);
      }
      else {
        //        LOG.info(queryPojo.getParams());
        return null;
      }

      //    if(queryPojo.getHits() == null) {
      //      // Log missing hits
      //      // How critical is this? Can this ever be missing
      //    }
      //    else {
      //      int hits = Integer.parseInt(queryPojo.getHits());
      //      instance.add((double) hits);
      //    }
      addBitmaskBooleanFields(instance, queryPojo.getBitmask());
      return instance.toArray(new Double[instance.size()]);
    }
    catch (Exception ignored){
      //      System.out.println("$$$$$$$$$$$ EXCEPTION  "+ queryPojo.getParams());
    }
    return  null;
  }


  private static void addBitmaskBooleanFields(ArrayList<Double> instance, String bitmask) {
    if(bitmask.length() != 7) {
      System.out.println("Invalid bitmask: " + bitmask);
      return;
    }

    instance.add(Double.parseDouble(String.valueOf(bitmask.charAt(0))));
    instance.add(Double.parseDouble(String.valueOf(bitmask.charAt(1))));
    instance.add(Double.parseDouble(String.valueOf(bitmask.charAt(2))));
    instance.add(Double.parseDouble(String.valueOf(bitmask.charAt(3))));
    instance.add(Double.parseDouble(String.valueOf(bitmask.charAt(4))));
    instance.add(Double.parseDouble(String.valueOf(bitmask.charAt(5))));
    instance.add(Double.parseDouble(String.valueOf(bitmask.charAt(6))));
  }


}

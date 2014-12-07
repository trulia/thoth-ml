package com.trulia.thoth.topicmodeling;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * User: dbraga - Date: 12/6/14
 */
public class Util {

  private static final String propertiesFileName = "application.default.properties";

  public static Properties fetchPropertiesFromPropertiesFile() throws IOException {
    Properties properties = new Properties();
    InputStream inputStream = Util.class.getClassLoader().getResourceAsStream(propertiesFileName);
    if (inputStream != null) {
      properties.load(inputStream);
    } else {
      throw new FileNotFoundException("property file '" + propertiesFileName + "' not found in the classpath");
    }
    return properties;
  }

}

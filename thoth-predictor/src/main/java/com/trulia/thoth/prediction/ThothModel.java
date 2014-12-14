package com.trulia.thoth.prediction;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;

/**
 * User: dbraga - Date: 10/2/14
 */
public class ThothModel {
  private static Logger LOG = Logger.getLogger(ThothModel.class);

  private static final String VERSION_FILE = "version";
  private String version = "-1";

  public String fetchLocalVersion(){
    if ("-1".equals(version)){
      LOG.info("Fetching local version in memory returned model version -1. Reading from file");
      FileReader file = null;
      try {
        file = new FileReader(VERSION_FILE);
        BufferedReader br = new BufferedReader(file);
        version = br.readLine();
        file.close();
        LOG.info(String.format("Read version(%s)",version));
      } catch (FileNotFoundException e) {
        version="-1";
      } catch (IOException e) {
        version="-1";
      }
    }
    return this.version;
  }

  public String fetchRemoteVersion(String URI){
    try {
      URL url = new URL(URI);
      URLConnection con = url.openConnection();
      InputStream in = con.getInputStream();
      String encoding = con.getContentEncoding();
      encoding = encoding == null ? "UTF-8" : encoding;
      String body = IOUtils.toString(in, encoding);
      return body;
    } catch (Exception e){
      return "0";
    }
  }

  public void setNewVersion(String v){
    File f = new File(VERSION_FILE);
    PrintWriter pw = null;
    try {
      pw = new PrintWriter(f);
      pw.write(v);
      pw.close();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
    // Set new version in memory
    this.version = v;
  }


}
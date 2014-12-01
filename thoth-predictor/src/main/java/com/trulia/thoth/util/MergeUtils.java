package com.trulia.thoth.util;

import java.io.*;

/**
 * User: dbraga - Date: 11/30/14
 */
public class MergeUtils {
  private BufferedWriter bw;
  private BufferedReader br;
  private String fileName;


  public MergeUtils(String fileName) throws UnsupportedEncodingException {
    this.fileName = fileName;

  }
}

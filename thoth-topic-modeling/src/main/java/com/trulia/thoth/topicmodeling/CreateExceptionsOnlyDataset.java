package com.trulia.thoth.topicmodeling;

import java.io.*;
import java.util.Properties;

/**
 * Created by pmhatre on 10/17/14.
 */
public class CreateExceptionsOnlyDataset {

  private static Properties properties;

  public static void main(String[] args) throws IOException, ClassNotFoundException {
    properties = Util.fetchPropertiesFromPropertiesFile();
    BufferedReader br = new BufferedReader(new FileReader(properties.getProperty("file.exceptions")));
    BufferedWriter bw = new BufferedWriter(new FileWriter(properties.getProperty("file.exceptions.mallet-format")));

    String line;
    int id = 0;
    while ( (line=br.readLine()) != null ) {
      String[] fields = line.split("\t");
      if(fields.length != 8) {
        System.out.println("Invalid line: " + fields.length + " fields");
        continue;
      }

      for(int i=0; i<8; i++) {
        if(i == 7) {
          bw.write((id++) + "\t" + "X" + "\t");
          bw.write(fields[i]);
          bw.newLine();
        }
      }
    }
    bw.flush();
  }
}
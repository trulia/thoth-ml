package com.trulia.thoth.topicmodeling;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by pmhatre on 10/17/14.
 */
public class CreateExceptionsOnlyDataset {

  public static void main(String[] args) throws IOException, ClassNotFoundException {

    BufferedReader br = new BufferedReader(new FileReader("/tmp/exceptions"));
    BufferedWriter bw = new BufferedWriter(new FileWriter("/tmp/exceptions-only"));

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
package com.trulia.thoth.util;

import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;

/**
 * User: dbraga - Date: 11/30/14
 */
public class MergeUtils {
  private String mergeFile;
  private String dirToMerge;
  private CircularFifoBuffer fifo;
  private static final Logger LOG = Logger.getLogger(MergeUtils.class);


  /**
   * Helper to merge multiple files to single file that works as a FIFO buffer
   * @param mergeFile merge output file
   * @param dirToMerge directory that contains the files to merge
   * @param lineCountLimit max number of lines allowed in the mergeFile. Older lines gets pushed out when limits is reached
   * @throws UnsupportedEncodingException
   */
  public MergeUtils(String mergeFile, String dirToMerge, int lineCountLimit) throws UnsupportedEncodingException {
    this.mergeFile = mergeFile;
    this.dirToMerge = dirToMerge;
    this.fifo = new CircularFifoBuffer(lineCountLimit);
  }

  /**
   * Add all the lines of the merged file to the FIFO buffer
   * @param file file to read
   * @param createFileIfMissing generate the file if missing
   */
  private void addFileLinesToFIFO(File file, boolean createFileIfMissing) {
    BufferedReader br  = null;
    try {
      if (!file.exists() && createFileIfMissing){
        file.createNewFile();
      }
      br = new BufferedReader(new FileReader(file));
      String line = br.readLine();
      while (line != null){
        if (!line.equals("")) fifo.add(line);
        line = br.readLine();
      }
      br.close();
    } catch (FileNotFoundException e) {

    } catch (IOException e) {
      e.printStackTrace();
    }
  }


  /**
   * Get a list of all the files contained in a dir
   * @param directoryName path of the dir
   * @return array list of files
   */
  public ArrayList<File> filesinDir(String directoryName){
    ArrayList<File> filelist = new ArrayList<File>();
    File directory = new File(directoryName);
    //get all the files from a directory
    File[] fList = directory.listFiles();
    if (fList == null){
      return null;
    }
    for (File file : fList){
      if (file.isFile()){
        filelist.add(file.getAbsoluteFile());
      }
    }
    return filelist;
  }


  /**
   * Takes care of merging the sampling files to a singe merged file that can be used by the predictor
   * in the training phase
   * @throws IOException
   */
  public void merge() throws IOException {
    // First add all the lines of the merged file to the FIFO buffer
    addFileLinesToFIFO(new File(mergeFile), true);
    // Get all the files that needs to be merged, append the lines of each file to the FIFO buffer
    ArrayList<File> filesToMerge = filesinDir(dirToMerge);
    if (filesToMerge == null){
      // No sampling directory found
      LOG.error("Sampling directory not found. Skipping ...");
      return;
    }
    for (File file: filesToMerge){
      LOG.info("Merging file: "  + file.getAbsoluteFile()+" to " + mergeFile);
      addFileLinesToFIFO(file, false);
      LOG.info("Merge finished. Deleting file: " + file.getAbsoluteFile() );
      file.delete();
    }
    // Write the FIFO buffer back to the merged file replacing what is there
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream( mergeFile, false),"UTF-8"));
    for (Object element: fifo){
      bw.write((String) element);
      bw.newLine();
    }
    bw.close();
  }

  public static void main(String[] args) throws IOException {
    MergeUtils merge = new MergeUtils("/tmp/merge", "/tmp/tomerge/", 5);
    merge.merge();
  }

}

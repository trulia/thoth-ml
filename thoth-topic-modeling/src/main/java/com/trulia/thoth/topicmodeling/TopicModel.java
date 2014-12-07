package com.trulia.thoth.topicmodeling;

import cc.mallet.pipe.*;
import cc.mallet.pipe.iterator.CsvIterator;
import cc.mallet.topics.ParallelTopicModel;
import cc.mallet.types.*;

import java.io.*;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Taken from Mallet's java developer guide
 */

public class TopicModel {
  private static Properties properties;

  public void testTopicModeling(int numTopics, int numIterations, int numKeywordsToOutput) throws IOException {
    System.out.println("Stop word file: " + properties.getProperty("file.stopWords"));
    System.out.println("Exceptions mallet format file: " + properties.getProperty("file.exceptions.mallet-format"));


    // Begin by importing documents from text to feature sequences
    ArrayList<Pipe> pipeList = new ArrayList<Pipe>();

    // Pipes: lowercase, tokenize, remove stopwords, map to features
    pipeList.add( new CharSequenceLowercase() );
    pipeList.add( new CharSequence2TokenSequence(Pattern.compile("\\p{L}[\\p{L}\\p{P}]+\\p{L}")) );

    pipeList.add( new TokenSequenceRemoveStopwords(new File(properties.getProperty("file.stopWords")), "UTF-8", false, false, false) );
    pipeList.add( new TokenSequence2FeatureSequence() );

    InstanceList instances = new InstanceList (new SerialPipes(pipeList));

    Reader fileReader = new InputStreamReader(new FileInputStream(properties.getProperty("file.exceptions.mallet-format")));

    instances.addThruPipe(new CsvIterator (fileReader, Pattern.compile("^(\\S*)[\\s,]*(\\S*)[\\s,]*(.*)$"),
      3, 2, 1)); // data, label, name fields

    //  alpha_t = 0.01, beta_w = 0.01
    ParallelTopicModel model = new ParallelTopicModel(numTopics, 1.0, 0.01);

    model.addInstances(instances);
    model.setNumThreads(2);
    model.setNumIterations(numIterations);
    model.estimate();

    // The data alphabet maps word IDs to strings
    Alphabet dataAlphabet = instances.getDataAlphabet();

    FeatureSequence tokens = (FeatureSequence) model.getData().get(0).instance.getData();
    LabelSequence topics = model.getData().get(0).topicSequence;

    Formatter out = new Formatter(new StringBuilder(), Locale.US);
    for (int position = 0; position < tokens.getLength(); position++) {
      out.format("%s-%d ", dataAlphabet.lookupObject(tokens.getIndexAtPosition(position)), topics.getIndexAtPosition(position));
    }
    System.out.println(out);


    // Get an array of sorted sets of word ID/count pairs
    ArrayList<TreeSet<IDSorter>> topicSortedWords = model.getSortedWords();

    // Show top 5 words in topics with proportions for the first document
    for (int topic = 0; topic < numTopics; topic++) {
      Iterator<IDSorter> iterator = topicSortedWords.get(topic).iterator();
      //TODO: parametrize this
      BufferedWriter bw = new BufferedWriter(new FileWriter(properties.getProperty("directory.topicModeling.visualization") + "topic-" +
        topic +
        ".csv"));
      bw.write("text,size,topic");
      bw.newLine();

      int rank = 0;
      while (iterator.hasNext() && rank < numKeywordsToOutput) {
        IDSorter idCountPair = iterator.next();
        bw.write(dataAlphabet.lookupObject(idCountPair.getID()) + "," + idCountPair.getWeight() + "," + topic);
        bw.newLine();
        rank++;
      }
      bw.flush();
    }

    // Overall distributions
    HashMap<Integer, Integer> topicToAssignedMap = new HashMap<Integer, Integer>();
    System.out.println();
    System.out.println("We have " + instances.size() + " instances");
    for(int i=0; i< instances.size(); i++) {
      double[] probs = model.getTopicProbabilities(i);
      double max = -1; int maxIndex = -1;
      for(int j = 0; j < numTopics; j++) {
        if(probs[j] > max) {
          max = probs[j];
          maxIndex = j;
        }
      }


      // update the counts hashmap
      int count = 0;
      if(topicToAssignedMap.containsKey(maxIndex)) {
        count = topicToAssignedMap.get(maxIndex);
      }
      count++;
      topicToAssignedMap.put(maxIndex, count);
    }

    for(int key: topicToAssignedMap.keySet()) {
      System.out.println("topic-" + key + ": " + topicToAssignedMap.get(key));
    }

  }

  public static void main(String[] args) throws Exception {
    properties = Util.fetchPropertiesFromPropertiesFile();
    int numTopics = 10;
    int numIterations = 100;
    int numKeywordsToOutput = 50;
    new TopicModel().testTopicModeling(numTopics, numIterations, numKeywordsToOutput);
  }

}
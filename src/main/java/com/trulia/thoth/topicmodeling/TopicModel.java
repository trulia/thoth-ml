package com.trulia.thoth.topicmodeling;

import cc.mallet.util.*;
import cc.mallet.types.*;
import cc.mallet.pipe.*;
import cc.mallet.pipe.iterator.*;
import cc.mallet.topics.*;

import java.util.*;
import java.util.regex.*;
import java.io.*;

/**
 * Taken from Mallet's java developer guide
 */

public class TopicModel {

  public static void main(String[] args) throws Exception {

    // Begin by importing documents from text to feature sequences
    ArrayList<Pipe> pipeList = new ArrayList<Pipe>();

    // Pipes: lowercase, tokenize, remove stopwords, map to features
    pipeList.add( new CharSequenceLowercase() );
    pipeList.add( new CharSequence2TokenSequence(Pattern.compile("\\p{L}[\\p{L}\\p{P}]+\\p{L}")) );
    pipeList.add( new TokenSequenceRemoveStopwords(new File("/Users/pmhatre/Downloads/mallet-2.0.6/stoplists/en.txt"), "UTF-8", false, false, false) );
    pipeList.add( new TokenSequence2FeatureSequence() );

    InstanceList instances = new InstanceList (new SerialPipes(pipeList));

    Reader fileReader = new InputStreamReader(new FileInputStream(new File
      ("/Users/pmhatre/thoth-data/exceptions-only")),
      "UTF-8");
    instances.addThruPipe(new CsvIterator (fileReader, Pattern.compile("^(\\S*)[\\s,]*(\\S*)[\\s,]*(.*)$"),
      3, 2, 1)); // data, label, name fields

    // Create a model with 100 topics, alpha_t = 0.01, beta_w = 0.01
    //  Note that the first parameter is passed as the sum over topics, while
    //  the second is the parameter for a single dimension of the Dirichlet prior.
    int numTopics = 10;
    ParallelTopicModel model = new ParallelTopicModel(numTopics, 1.0, 0.01);

    model.addInstances(instances);

    // Use two parallel samplers, which each look at one half the corpus and combine
    //  statistics after every iteration.
    model.setNumThreads(2);

    // Run the model for 50 iterations and stop (this is for testing only,
    //  for real applications, use 1000 to 2000 iterations)
    model.setNumIterations(50);
    model.estimate();

    // Show the words and topics in the first instance

    // The data alphabet maps word IDs to strings
    Alphabet dataAlphabet = instances.getDataAlphabet();

    FeatureSequence tokens = (FeatureSequence) model.getData().get(0).instance.getData();
    LabelSequence topics = model.getData().get(0).topicSequence;

    Formatter out = new Formatter(new StringBuilder(), Locale.US);
    for (int position = 0; position < tokens.getLength(); position++) {
      out.format("%s-%d ", dataAlphabet.lookupObject(tokens.getIndexAtPosition(position)), topics.getIndexAtPosition(position));
    }
    System.out.println(out);

    // Estimate the topic distribution of the first instance,
    //  given the current Gibbs state.
    double[] topicDistribution = model.getTopicProbabilities(0);

    // Get an array of sorted sets of word ID/count pairs
    ArrayList<TreeSet<IDSorter>> topicSortedWords = model.getSortedWords();

    // Show top 5 words in topics with proportions for the first document
    for (int topic = 0; topic < numTopics; topic++) {
      Iterator<IDSorter> iterator = topicSortedWords.get(topic).iterator();
      BufferedWriter bw = new BufferedWriter(new FileWriter("/Users/pmhatre/thoth-data/topic-modeling/tempTopic-" +
        topic +
        ".csv"));
      bw.write("text,size,topic");
      bw.newLine();

      out = new Formatter(new StringBuilder(), Locale.US);
      out.format("%d\t%.3f\t", topic, topicDistribution[topic]);
      int rank = 0;
      while (iterator.hasNext() && rank < 100) {
        IDSorter idCountPair = iterator.next();
//        out.format("%s (%.0f) ", dataAlphabet.lookupObject(idCountPair.getID()), idCountPair.getWeight());
//        out.format("%s,%.0f,%d\n", dataAlphabet.lookupObject(idCountPair.getID()), idCountPair.getWeight(), topic);
        bw.write(dataAlphabet.lookupObject(idCountPair.getID()) + "," + idCountPair.getWeight() + "," + topic);
        bw.newLine();
        rank++;
      }
      bw.flush();
//      System.out.println(out);
    }

    // Create a new instance with high probability of topic 0
    StringBuilder topicZeroText = new StringBuilder();
    Iterator<IDSorter> iterator = topicSortedWords.get(0).iterator();

    int rank = 0;
    while (iterator.hasNext() && rank < 5) {
      IDSorter idCountPair = iterator.next();
      topicZeroText.append(dataAlphabet.lookupObject(idCountPair.getID()) + " ");
      rank++;
    }

    // Create a new instance named "test instance" with empty target and source fields.
    InstanceList testing = new InstanceList(instances.getPipe());
    testing.addThruPipe(new Instance(topicZeroText.toString(), null, "test instance", null));

    TopicInferencer inferencer = model.getInferencer();
    double[] testProbabilities = inferencer.getSampledDistribution(testing.get(0), 10, 1, 5);
    System.out.println("0\t" + testProbabilities[0]);

    // Overall distributions
    HashMap<Integer, Integer> topicToAssignedMap = new HashMap<Integer, Integer>();
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
      System.out.println(key + ": " + topicToAssignedMap.get(key));
    }

  }

}
package com.trulia.thoth.quartz;

import com.trulia.thoth.MergeUtils;
import com.trulia.thoth.pojo.ServerDetail;
import com.trulia.thoth.predictor.ModelHealth;
import com.trulia.thoth.predictor.StaticModelHealth;
import com.trulia.thoth.util.IgnoredServers;
import com.trulia.thoth.util.ThothServers;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.quartz.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * User: dbraga - Date: 7/21/14
 */
public class ThothSampler implements Job {
  private static final Logger LOG = Logger.getLogger(ThothSampler.class);
  private List<ServerDetail> serversDetail = null;
  private List<ServerDetail> ignored = null;
  private List<Future> futureList;
  private String mergeDirectory;
  private String samplingDirectory;
  private ObjectMapper mapper;
  private int lineCountLimit;
  private String ignoredServersList;


  public ArrayList<File> filesinDir(String directoryName){
    ArrayList<File> filelist = new ArrayList<File>();
    File directory = new File(directoryName);
    //get all the files from a directory
    File[] fList = directory.listFiles();
    for (File file : fList){
      if (file.isFile()){
        filelist.add(file.getAbsoluteFile());
      }
    }
    return filelist;
  }

  /**
   * Merge all sampling files into a single file
   */
  private void mergeSamplingFiles(){
    try {
      new MergeUtils(MergeUtils.getMergeFile(mergeDirectory), samplingDirectory, lineCountLimit).merge();
      LOG.info("Merge complete.");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void execute(JobExecutionContext context) throws JobExecutionException {
    SchedulerContext schedulerContext = null;
    try {
      mapper = new ObjectMapper();
      mapper.configure(SerializationConfig.Feature.WRITE_NULL_PROPERTIES, false);
      schedulerContext = context.getScheduler().getContext();
      mergeDirectory = (String)schedulerContext.get("mergingDir");
      samplingDirectory = (String)schedulerContext.get("samplingDir");
      lineCountLimit = (Integer)schedulerContext.get("lineCountLimit");
      ignoredServersList = (String)schedulerContext.get("ignoredServersList");

      ModelHealth modelHealth = (ModelHealth)schedulerContext.get("modelHealth");
      HttpSolrServer thothIndex = new HttpSolrServer((String)schedulerContext.get("thothIndex"));
      ThothServers thothServers = new ThothServers();

      //TODO: To remove ASAP  - BEST-1377
      StaticModelHealth userStaticModelHealth = (StaticModelHealth)schedulerContext.get("userStaticModelHealth");
      StaticModelHealth drStaticModelHealth = (StaticModelHealth)schedulerContext.get("drStaticModelHealth");
      StaticModelHealth mobileStaticModelHealth = (StaticModelHealth)schedulerContext.get("mobileStaticModelHealth");
      StaticModelHealth googleStaticModelHealth = (StaticModelHealth)schedulerContext.get("googleStaticModelHealth");

      serversDetail = thothServers.getList(thothIndex);
      IgnoredServers ignoredServers = new IgnoredServers(ignoredServersList);
      ignored = ignoredServers.getIgnoredServersDetail();

      //TODO: why 10?
      ExecutorService service = Executors.newFixedThreadPool(10);
      futureList = new ArrayList<Future>();
      CompletionService<String> ser = new ExecutorCompletionService<String>(service);

      File dir = new File(samplingDirectory);
      new File(mergeDirectory).mkdirs();
      boolean success = (dir).mkdirs();
      if (success || dir.exists() ) {

        for (ServerDetail server: serversDetail){

          if (ignoredServers.isServerIgnored(server)) continue;

          try {
            Future<String> future = ser.submit(new SamplerWorker(
                server,
                samplingDirectory,
                mapper,
                thothIndex,
                modelHealth,
                userStaticModelHealth,
                drStaticModelHealth,
                mobileStaticModelHealth,
                googleStaticModelHealth
            ));
            futureList.add(future);
          } catch (IOException e) {
            e.printStackTrace();
          }

        }



        for(Future<String> fut : futureList){
          try {
            fut.get();
          } catch (Exception e) {
            e.printStackTrace();
          }
        }

        LOG.info("Done Sampling. Merging single files into one");
        mergeSamplingFiles();


      } else {
        LOG.error("Coudn't create directory");
      }

    } catch (SchedulerException e) {
      e.printStackTrace();
    } catch (SolrServerException e) {
      e.printStackTrace();
    }

  }

}

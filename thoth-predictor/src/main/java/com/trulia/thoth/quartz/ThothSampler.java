package com.trulia.thoth.quartz;

import com.trulia.thoth.pojo.ServerDetail;
import com.trulia.thoth.predictor.ModelHealth;
import com.trulia.thoth.predictor.StaticModelHealth;
import com.trulia.thoth.util.ThothServers;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.quartz.*;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
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

  private boolean isIgnored(ServerDetail serverDetail){
    for (ServerDetail toCheck: ignored){
      if ((toCheck.getName().equals(serverDetail.getName())) &&
          (toCheck.getCore().equals(serverDetail.getCore())) &&
          (toCheck.getPool().equals(serverDetail.getPool())) &&
          (toCheck.getPort().equals(serverDetail.getPort()))){
        return true;
      }
    }
    return false;
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
      ModelHealth modelHealth = (ModelHealth)schedulerContext.get("modelHealth");
      HttpSolrServer thothIndex = new HttpSolrServer((String)schedulerContext.get("thothIndex"));
      ThothServers thothServers = new ThothServers();

      //TODO: To remove ASAP  - BEST-1377
      StaticModelHealth userStaticModelHealth = (StaticModelHealth)schedulerContext.get("userStaticModelHealth");
      StaticModelHealth drStaticModelHealth = (StaticModelHealth)schedulerContext.get("drStaticModelHealth");
      StaticModelHealth mobileStaticModelHealth = (StaticModelHealth)schedulerContext.get("mobileStaticModelHealth");
      StaticModelHealth googleStaticModelHealth = (StaticModelHealth)schedulerContext.get("googleStaticModelHealth");

      serversDetail = thothServers.getList(thothIndex);

      //TODO: still need ignored servers?
      ignored  = new ArrayList<ServerDetail>();
      ignored.add(new ServerDetail("search200", "user","8050","active"));
      ignored.add(new ServerDetail("search228", "user","8050","active"));
      ignored.add(new ServerDetail("search254", "user","8050","active"));
      ignored.add(new ServerDetail("search255", "user","8050","active"));
      ignored.add(new ServerDetail("search39", "user","8050","active"));

      ignored.add(new ServerDetail("search42", "user","8050","active"));
      ignored.add(new ServerDetail("search43", "user","8050","active"));
      ignored.add(new ServerDetail("search44", "user","8050","active"));
      ignored.add(new ServerDetail("search252", "user","8050","active"));
      ignored.add(new ServerDetail("search253", "user","8050","active"));

      //TODO: why 10?
      ExecutorService service = Executors.newFixedThreadPool(10);
      futureList = new ArrayList<Future>();
      CompletionService<String> ser = new ExecutorCompletionService<String>(service);

      File dir = new File(samplingDirectory);
      new File(mergeDirectory).mkdirs();
      boolean success = (dir).mkdirs();
      if (success || dir.exists() ) {

        for (ServerDetail server: serversDetail){

          if (isIgnored(server)) continue;

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
        try {
          DateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd");
          Date date = new Date();
          String mergeFile = mergeDirectory +  dateFormat.format(date)+"_merged";
          BufferedWriter bufferedWriter =  new BufferedWriter((new OutputStreamWriter(new FileOutputStream( mergeFile, true),"UTF-8")));


          for (File file: filesinDir(samplingDirectory)){
            LOG.info("Merging file: "  + file.getAbsoluteFile()+" to " + mergeFile);
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line;
            while ((line = reader.readLine()) != null) {
              bufferedWriter.write(line);
              bufferedWriter.write("\n");
            }
            reader.close();
            LOG.info("Merge finished. Deleting file: " + file.getAbsoluteFile() );
            file.delete();

          }
          bufferedWriter.close();



          LOG.info("Merge complete.");
        } catch (IOException e) {
          e.printStackTrace();
        }


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

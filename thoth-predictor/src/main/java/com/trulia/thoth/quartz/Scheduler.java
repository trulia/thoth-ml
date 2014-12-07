package com.trulia.thoth.quartz;

import com.trulia.thoth.predictor.ModelHealth;
import org.apache.log4j.Logger;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

/**
 * User: dbraga - Date: 7/21/14
 */
public class Scheduler {
  private static final Logger LOG = Logger.getLogger(Scheduler.class);

  private boolean samplingEnabled;
  private String samplingDir;
  private String mergingDir;
  private String serverDetails;
  private String thothIndex;
  private String schedule;
  private ModelHealth modelHealth;
  private int lineCountLimit;
  private String ignoredServers;

  public void init() throws SchedulerException {
    if (samplingEnabled){
      LOG.info("Thoth sampling enabled.");

      // Quartz Setup for resetting
      JobDetail workerJob = JobBuilder.newJob(ThothSampler.class)
          .withIdentity("samplerJob", "group1").build();
      Trigger workerTrigger = TriggerBuilder
          .newTrigger()
          .withIdentity("samplerTrigger", "group1")
          .withSchedule(
              CronScheduleBuilder.cronSchedule(schedule))
          .build();

      //Schedule it
      org.quartz.Scheduler scheduler = new StdSchedulerFactory().getScheduler();
      scheduler.start();
      scheduler.getContext().put("samplingDir", samplingDir);
      scheduler.getContext().put("mergingDir", mergingDir);
      scheduler.getContext().put("lineCountLimit", lineCountLimit);
      scheduler.getContext().put("ignoredServers", ignoredServers);
      scheduler.getContext().put("thothIndex",thothIndex);
      scheduler.getContext().put("modelHealth",modelHealth);
      scheduler.scheduleJob(workerJob, workerTrigger);
    } else {
      LOG.info("Sampling disabled. Skipping.");
    }
  }


  public boolean isSamplingEnabled() {
    return samplingEnabled;
  }

  public void setSamplingEnabled(boolean samplingEnabled) {
    this.samplingEnabled = samplingEnabled;
  }


  public void setSamplingDir(String samplingDir) {
    this.samplingDir = samplingDir;
  }

  public String getSamplingDir() {
    return samplingDir;
  }

  public void setMergingDir(String mergingDir) {
    this.mergingDir = mergingDir;
  }

  public String getMergingDir() {
    return mergingDir;
  }

  public void setServerDetails(String serverDetails) {
    this.serverDetails = serverDetails;
  }

  public String getServerDetails() {
    return serverDetails;
  }

  public void setThothIndex(String thothIndex) {
    this.thothIndex = thothIndex;
  }

  public String getThothIndex() {
    return thothIndex;
  }

  public void setSchedule(String schedule) {
    this.schedule = schedule;
  }

  public String getSchedule() {
    return schedule;
  }

  public void setModelHealth(ModelHealth modelHealth) {
    this.modelHealth = modelHealth;
  }

  public ModelHealth getModelHealth() {
    return modelHealth;
  }

  public void setLineCountLimit(int lineCountLimit) {
    this.lineCountLimit = lineCountLimit;
  }

  public int getLineCountLimit() {
    return lineCountLimit;
  }

  public void setIgnoredServers(String ignoredServers) {
    this.ignoredServers = ignoredServers;
  }

  public String getIgnoredServers() {
    return ignoredServers;
  }
}

package uk.co.bocuma.wikipedia.config;

public class WikipediaConfig {
  private String brokerHost;
  private String exchangeName;
  private String outputDir;
  private String processJobQueueName;
  public static class States {
    public static final String NEW = "0";
    public static final String DOWNLOADING = "1";
    public static final String DOWNLOADED = "2";
    public static final String PROCESSING = "3";
    public static final String PROCESSED = "4";
    public static final String COMPLETED = "5";
    public static final String ERROR = "-1";
  }
  public WikipediaConfig() {
    this.brokerHost = "localhost";
    this.exchangeName = "wikipedia";
    this.outputDir = "/tmp/wikipedia";
    this.processJobQueueName = "jobs";
  }

  public String getBrokerHost() {
    return this.brokerHost;
  }

  public String getExchangeName() {
    return this.exchangeName;
  }

  public String getOutputDir() {
    return this.outputDir;
  }

  public String mrInputFile(String title,String language) {
    return getOutputDir() + "/" + language + "-" + title + ".xml";
  }
  public String mrOutputFile(String title,String language) {
    return getOutputDir() + "/" + language + "-" +  title + ".out";
  }


  public String processJobQueueName() {
    return this.processJobQueueName;
  }
}

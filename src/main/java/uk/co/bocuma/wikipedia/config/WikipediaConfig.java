package uk.co.bocuma.wikipedia.config;

public class WikipediaConfig {
  private String brokerHost;
  private String brokerQueue;
  private String outputDir;
  public WikipediaConfig() {
    this.brokerHost = "localhost";
    this.brokerQueue = "wikipedia";
    this.outputDir = "/tmp/wikipedia";
  }

  public String getBrokerHost() {
    return this.brokerHost;
  }

  public String getBrokerQueue() {
    return this.brokerQueue;
  }

  public String getOutputDir() {
    return this.outputDir;
  }
}

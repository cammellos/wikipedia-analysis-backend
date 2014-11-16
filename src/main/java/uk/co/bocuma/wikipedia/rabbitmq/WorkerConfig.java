package uk.co.bocuma.wikipedia.rabbitmq;

public class WorkerConfig {
  private String host;
  private String queue;
  public WorkerConfig() {
    this.host = "localhost";
    this.queue = "wikipedia";
  }
  public WorkerConfig(String host, String queue) {
    this.host = host;
    this.queue = queue;
  }

  public String getHost() {
    return this.host;
  }

  public String getQueue() {
    return this.queue;
  }


}

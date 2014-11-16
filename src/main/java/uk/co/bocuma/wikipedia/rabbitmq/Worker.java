package uk.co.bocuma.wikipedia.rabbitmq;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;
import uk.co.bocuma.wikipedia.hadoop.MapReduce;
import uk.co.bocuma.wikipedia.config.WikipediaConfig;
import uk.co.bocuma.wikipedia.http.Downloader;
import java.io.*;

public class Worker {

  private WikipediaConfig config;
  private Channel channel;

  public Worker() {
    this(new WikipediaConfig());
  }

  public Worker(WikipediaConfig config) {
    this.config = config;
  }


  public void run() throws java.io.IOException,
             java.lang.InterruptedException {
    this.channel = createChannel();

    channel.queueDeclare(config.getBrokerQueue(), false, false, false, null);

    QueueingConsumer consumer = createConsumer();
    this.channel.basicConsume(this.config.getBrokerQueue(), true, consumer );
    while (true) {
      QueueingConsumer.Delivery delivery = consumer.nextDelivery();
      String page = new String(delivery.getBody());
      try {
        downloadPage(page);
      } catch( Exception e) {
      }
      try {
        processPage(page);
      } catch ( Exception e) {
      }
      this.channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
    }
  }

  private Channel createChannel() throws java.io.IOException {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(this.config.getBrokerHost());
    Connection connection = factory.newConnection();
    return connection.createChannel();
  }

  private void downloadPage(String page) {
    Downloader d = new Downloader(config,page);
    d.download();
  }

  private void processPage(String page) throws Exception  {
    MapReduce.runJob(this.config.getOutputDir() + page + ".xml", this.config.getOutputDir() + page + ".out");
  }


  private QueueingConsumer createConsumer() {
    return new QueueingConsumer(this.channel);
  }

}

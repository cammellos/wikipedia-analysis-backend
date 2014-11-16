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

    this.channel.exchangeDeclare(this.config.getExchangeName(),"direct");
    String queueName = channel.queueDeclare().getQueue();

    this.channel.queueBind(queueName, this.config.getExchangeName(),this.config.processJobQueueName());
    QueueingConsumer consumer = createConsumer();
    this.channel.basicConsume(queueName, true, consumer );
    while (true) {
      QueueingConsumer.Delivery delivery = consumer.nextDelivery();
      String page = new String(delivery.getBody());
      String routingKey = delivery.getEnvelope().getRoutingKey();
      try {
        channel.basicPublish(this.config.getExchangeName(), "status", null, stateMessage(page, WikipediaConfig.States.DOWNLOADING).getBytes());
        downloadPage(page);
        channel.basicPublish(this.config.getExchangeName(), "status", null, stateMessage(page, WikipediaConfig.States.DOWNLOADED).getBytes());
        channel.basicPublish(this.config.getExchangeName(), "status", null, stateMessage(page, WikipediaConfig.States.PROCESSING).getBytes());
        processPage(page);
        channel.basicPublish(this.config.getExchangeName(), "status", null, stateMessage(page, WikipediaConfig.States.PROCESSED).getBytes());
        channel.basicPublish(this.config.getExchangeName(), "status", null, stateMessage(page, WikipediaConfig.States.COMPLETED).getBytes());
      } catch ( Exception e) {
        channel.basicPublish(this.config.getExchangeName(), "status", null, stateMessage(page, WikipediaConfig.States.ERROR).getBytes());
      }
      //this.channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
    }
  }

  private String stateMessage(String page,String status) {
    return page + "-" + status;
  }

  private Channel createChannel() throws java.io.IOException {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(this.config.getBrokerHost());
    Connection connection = factory.newConnection();
    return connection.createChannel();
  }

  private void downloadPage(String page) throws Exception {
    Downloader d = new Downloader(config,page);
    d.download();
  }

  private void processPage(String page) throws Exception  {
    MapReduce.runJob(this.config.getOutputDir() + "/" + page + ".xml", this.config.getOutputDir() + "/" + page + ".out");
  }


  private QueueingConsumer createConsumer() {
    return new QueueingConsumer(this.channel);
  }

}

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

      String[] messages = new String(delivery.getBody()).split("*");

      String id = messages[0];
      String language = messages[1];
      String title = messages[2];

      String routingKey = delivery.getEnvelope().getRoutingKey();

      try {

        publishState(id, WikipediaConfig.States.DOWNLOADING);
        downloadPage(title,language);
        publishState(id, WikipediaConfig.States.DOWNLOADED);

        publishState(id, WikipediaConfig.States.PROCESSING);
        processPage(title,language);
        publishState(id, WikipediaConfig.States.PROCESSED);

        publishState(id, WikipediaConfig.States.COMPLETED);
      } catch ( Exception e) {

        publishState(id, WikipediaConfig.States.ERROR);

      }
      //this.channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
    }
  }

  private void publishState(String url,String state) throws IOException {
     channel.basicPublish(this.config.getExchangeName(), "status", null, stateMessage(url, state).getBytes());
  }

  private String stateMessage(String id,String status) {
    return id + "-" + status;
  }

  private Channel createChannel() throws java.io.IOException {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(this.config.getBrokerHost());
    Connection connection = factory.newConnection();
    return connection.createChannel();
  }

  private void downloadPage(String title, String language) throws Exception {
    Downloader d = new Downloader(config,title,language);
    d.download();
  }

  private void processPage(String title,String language) throws Exception  {
    MapReduce.runJob(this.config.mrInputFile(title,language), this.config.mrOutputFile(title,language));
  }


  private QueueingConsumer createConsumer() {
    return new QueueingConsumer(this.channel);
  }

}

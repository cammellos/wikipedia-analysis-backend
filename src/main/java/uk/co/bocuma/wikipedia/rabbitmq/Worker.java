package uk.co.bocuma.wikipedia.rabbitmq;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;

public class Worker {

  private WorkerConfig config;
  private Channel channel;

  public Worker(WorkerConfig config) {
    this.config = config;
  }


  public void run() throws java.io.IOException,
             java.lang.InterruptedException {
    this.channel = createChannel();

    channel.queueDeclare(config.getQueue(), false, false, false, null);

    QueueingConsumer consumer = createConsumer();
    this.channel.basicConsume(this.config.getQueue(), true, consumer );
    while (true) {
      QueueingConsumer.Delivery delivery = consumer.nextDelivery();
      String message = new String(delivery.getBody());
      System.out.println(" [x] Received '" + message + "'");
      this.channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
    }
  }

  private Channel createChannel() throws java.io.IOException {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(this.config.getHost());
    Connection connection = factory.newConnection();
    return connection.createChannel();
  }

  private QueueingConsumer createConsumer() {
    return new QueueingConsumer(this.channel);
  }

}

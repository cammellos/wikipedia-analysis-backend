
package uk.co.bocuma.wikipedia;

import uk.co.bocuma.wikipedia.rabbitmq.Worker;

class Main {
  public static void main(String... args) throws Exception {
      Worker worker = new Worker();
      worker.run();
       }
}


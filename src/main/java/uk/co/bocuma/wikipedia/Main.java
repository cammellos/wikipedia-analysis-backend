
package uk.co.bocuma.wikipedia;

import uk.co.bocuma.wikipedia.rabbitmq.Worker;

class Main {
  public static void main(String... args) throws Exception {
    try {
      Worker worker = new Worker();
      worker.run();
    } catch(java.io.IOException e) {
      System.err.println(e.getMessage());
    } catch(java.lang.InterruptedException e) {
      System.err.println(e.getMessage());
    }
  }
}



package uk.co.bocuma.wikipedia;

import uk.co.bocuma.wikipedia.hadoop.MapReduce;

class Main {
  public static void main(String... args) throws Exception {
    MapReduce.runJob(args[0], args[1]);
  }
}


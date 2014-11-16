package uk.co.bocuma.wikipedia.http;

import uk.co.bocuma.wikipedia.config.WikipediaConfig;

public class Downloader  {
  private WikipediaConfig config;
  private String page;
  public Downloader(WikipediaConfig config,String page) {
    this.config = config;
    this.page = page;
  }

  public void download() {
  }

}

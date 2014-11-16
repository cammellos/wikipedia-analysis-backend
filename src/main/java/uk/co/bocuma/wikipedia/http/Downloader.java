package uk.co.bocuma.wikipedia.http;

import uk.co.bocuma.wikipedia.config.WikipediaConfig;
import org.apache.commons.io.IOUtils;
import java.net.*;
import java.io.FileOutputStream;


public class Downloader  {
  private WikipediaConfig config;
  private String page;

  private static final String BASE_URL = "http://en.wikipedia.org/w/index.php?title=Special:Export&pages=token%0ATalk:token&offset=1&action=submit";

  public Downloader(WikipediaConfig config,String page) {
    this.config = config;
    this.page = page;
  }

  public void download() throws Exception {
   URL url = new URL(buildUrl());
   HttpURLConnection connection = (HttpURLConnection) url.openConnection();
   connection.setRequestMethod("POST");
   IOUtils.copy(connection.getInputStream(), new FileOutputStream(buildOutputName()));
  }

  private String buildUrl() {
    return Downloader.BASE_URL.replaceAll("token",page);
  }
  private String buildOutputName() {
    return this.config.getOutputDir() + "/" + this.page + ".xml";
  }


}

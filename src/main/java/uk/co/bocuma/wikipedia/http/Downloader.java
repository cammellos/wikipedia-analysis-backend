package uk.co.bocuma.wikipedia.http;

import uk.co.bocuma.wikipedia.config.WikipediaConfig;
import org.apache.commons.io.IOUtils;
import java.net.*;
import java.io.FileOutputStream;


public class Downloader  {
  private WikipediaConfig config;
  private String title;
  private String language;

  private static final String BASE_URL = "http://**language**.wikipedia.org/w/index.php?title=Special:Export&pages=**token**%0ATalk:**token**&offset=1&action=submit";

  public Downloader(WikipediaConfig config,String title, String language) {
    this.config = config;
    this.title = title;
    this.language = language;
  }

  public void download() throws Exception {
   URL url = new URL(buildUrl());
   HttpURLConnection connection = (HttpURLConnection) url.openConnection();
   connection.setRequestMethod("POST");
   IOUtils.copy(connection.getInputStream(), new FileOutputStream(config.mrInputFile(title,language)));
  }

  private String buildUrl() {
    return Downloader.BASE_URL.replaceAll("**token**",title).replaceAll("**language**",language);
  }


}

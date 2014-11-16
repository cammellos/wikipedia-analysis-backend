package uk.co.bocuma.wikipedia.hadoop;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.slf4j.*;


import javax.xml.stream.*;
import java.io.*;

import static javax.xml.stream.XMLStreamConstants.*;

public final class MapReduce {
  private static final Logger log = LoggerFactory.getLogger
      (MapReduce.class);
  private final static IntWritable one = new IntWritable(1);

  public static class UserEditMap extends Mapper<Object, Text,
      UserEdit, IntWritable> {

    @Override
    protected void map(Object key, Text value,
                       Mapper.Context context)
        throws
        IOException, InterruptedException {
      String document = value.toString();
      try {
        XMLStreamReader reader =
            XMLInputFactory.newInstance().createXMLStreamReader(new
                ByteArrayInputStream(document.getBytes()));
        String currentElement = "";
        String ip = "";
        String username = "";
        String id = "";
        String timestamp = "";
        while (reader.hasNext()) {
          int code = reader.next();
          switch (code) {
            case START_ELEMENT:
              currentElement = reader.getLocalName();
              break;
            case CHARACTERS:
              if (currentElement.equalsIgnoreCase("ip")) {
                ip += reader.getText().trim();
              } else if (currentElement.equalsIgnoreCase("username")) {
                username += reader.getText().trim();
              } else if (currentElement.equalsIgnoreCase("id")) {
                id += reader.getText().trim();
              } else if (currentElement.equalsIgnoreCase("timestamp")) {
                timestamp += reader.getText().trim();
              }
              break;
          }
        }
        User user = new User(ip,username,id);
        UserEdit userEdit = new UserEdit(user,timestamp);
        context.write(userEdit, one);
        reader.close();
        
      } catch (Exception e) {
        log.error("Error processing '" + document + "'", e);
      }
    }
  }

  public static class UserEditReduce
         extends Reducer<UserEdit,IntWritable,Text,IntWritable> {
      private IntWritable result = new IntWritable();
      private static Text text = new Text();


      public void reduce(UserEdit userEdit, Iterable<IntWritable> values,
                         Context context
                         ) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
          sum += val.get();
        }
        result.set(sum);
        text.set(userEdit.toString());
        context.write(text, result);
      }
    }


  public static class UserTotalMap extends Mapper<Object, Text,
      User, IntWritable> {

    @Override
    protected void map(Object key, Text value,
                       Mapper.Context context)
        throws
        IOException, InterruptedException {
      String document = value.toString();
      try {
        XMLStreamReader reader =
            XMLInputFactory.newInstance().createXMLStreamReader(new
                ByteArrayInputStream(document.getBytes()));
        String currentElement = "";
        String ip = "";
        String username = "";
        String id = "";
        while (reader.hasNext()) {
          int code = reader.next();
          switch (code) {
            case START_ELEMENT:
              currentElement = reader.getLocalName();
              break;
            case CHARACTERS:
              if (currentElement.equalsIgnoreCase("ip")) {
                ip += reader.getText().trim();
              } else if (currentElement.equalsIgnoreCase("username")) {
                username += reader.getText().trim();
              } else if (currentElement.equalsIgnoreCase("id")) {
                id += reader.getText().trim();
              }
              break;
          }
        }
        User user = new User(ip,username,id);
        context.write(user, one);

        reader.close();
        
      } catch (Exception e) {
        log.error("Error processing '" + document + "'", e);
      }
    }
  }

  public static class UserTotalReduce
         extends Reducer<User,IntWritable,Text,IntWritable> {
      private IntWritable result = new IntWritable();
      private static Text text = new Text();


      public void reduce(User user, Iterable<IntWritable> values,
                         Context context
                         ) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
          sum += val.get();
        }
        result.set(sum);
        text.set(user.toString());
        context.write(text, result);
      }
    }

  public static void runJob(String input,
                            String output)
      throws Exception {
    Configuration conf = new Configuration();
    conf.set("key.value.separator.in.input.line", " ");
    conf.set("xmlinput.start", "<revision>");
    conf.set("xmlinput.end", "</revision>");
    conf.set("mapreduce.textoutputformat.separator", ",");
    conf.set("mapreduce.output.key.field.separator", ",");
    conf.set("mapred.textoutputformat.separator",",");

    Job job = new Job(conf);
    job.setJarByClass(MapReduce.class);

    configureUserEditMap(job);

    FileInputFormat.setInputPaths(job, new Path(input));
    Path outPath = new Path(output);
    FileOutputFormat.setOutputPath(job, outPath);
    outPath.getFileSystem(conf).delete(outPath, true);

    job.waitForCompletion(true);
  }

  private static void configureUserTotalMap(Job job) {
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setMapperClass(UserTotalMap.class);
    job.setReducerClass(UserTotalReduce.class);
    job.setInputFormatClass(XmlInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setMapOutputKeyClass(User.class);
    job.setMapOutputValueClass(IntWritable.class);
  }
  private static void configureUserEditMap(Job job) {
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setMapperClass(UserEditMap.class);
    job.setReducerClass(UserEditReduce.class);
    job.setInputFormatClass(XmlInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setMapOutputKeyClass(UserEdit.class);
    job.setMapOutputValueClass(IntWritable.class);
  }

}

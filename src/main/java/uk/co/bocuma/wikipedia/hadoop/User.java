
package uk.co.bocuma.wikipedia.hadoop;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;

import org.apache.hadoop.io.WritableComparable;
import java.io.*;

public class User implements WritableComparable {
  private Text ip;
  private Text username;
  private Text id;

  public User() {
    this.ip = new Text();
    this.username = new Text();
    this.id = new Text();
  }

  public User(String ip, String username, String id) {
    this.ip = new Text(ip);
    this.username = new Text(username);
    this.id = new Text(id);
  }

  public User(Text ip, Text username, Text id) {
    this.ip = ip;
    this.username = username;
    this.id = id;
  }

  public void setIp(String ip) {
    this.ip.set(ip);
  }
  public void setId(String id) {
    this.id.set(id);
  }
  public void setUsername(String username) {
    this.username.set(username);
  }

  public Text getIp() {
    return this.ip;
  }
  public Text getId() {
    return this.id;
  }
  public Text getUsername() {
    return this.username;
  }



  public void write(DataOutput out) throws IOException {
    ip.write(out);
    username.write(out);
    id.write(out);
  }

  public void readFields(DataInput in) throws IOException {
    ip.readFields(in);
    username.readFields(in);
    id.readFields(in);
  }

  public String toString() {
    return ip + ", "
        + username + ", "
        + id;
  }


  public int compareTo(Object o) {
    User other = (User)o;
    int cmp = ip.compareTo(other.getIp());

    if (cmp != 0) {
        return cmp;
    }

    return username.compareTo(other.getUsername());
  }

  public boolean equals(Object o) {
    if (!(o instanceof User)) {
      return false;
    }

    User other = (User)o;
    return other.getIp().equals(ip) && other.getId().equals(id) && other.getUsername().equals(username);
  }

  public int hashCode() {
    return ip.hashCode() * 1000 + id.hashCode() * 100 + username.hashCode() * 10;
  }
}

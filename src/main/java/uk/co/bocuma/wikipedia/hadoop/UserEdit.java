
package uk.co.bocuma.wikipedia.hadoop;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;

import org.apache.hadoop.io.WritableComparable;
import java.io.*;

public class UserEdit implements WritableComparable {
  private User user;
  private Text timestamp;

  public UserEdit() {
    this.user = new User();
    this.timestamp = new Text();
  }

  public UserEdit(User user,String timestamp) {
    this(user,new Text(timestamp));
  }

  public UserEdit(User user, Text timestamp) {
    this.user = user;
    this.timestamp = timestamp;
  }

  public void setUser(User user) {
    this.user = user;
  }
  public void setTimestamp(String timestamp) {
    this.timestamp.set(timestamp);
  }
  public User getUser() {
    return this.user;
  }
  public Text getTimestamp() {
    return this.timestamp;
  }

  public void write(DataOutput out) throws IOException {
    user.write(out);
    timestamp.write(out);
  }

  public void readFields(DataInput in) throws IOException {
    user.readFields(in);
    timestamp.readFields(in);
  }

  public String toString() {
    return user.toString() + ", " + timestamp;
  }


  public int compareTo(Object o) {
    UserEdit other = (UserEdit)o;
    int cmp = user.compareTo(other.getUser());

    if (cmp != 0) {
        return cmp;
    }

    return timestamp.compareTo(other.getTimestamp());
  }

  public boolean equals(Object o) {
    if (!(o instanceof UserEdit)) {
      return false;
    }

    UserEdit other = (UserEdit)o;
    return other.getUser().equals(this.user) && other.getTimestamp().equals(this.timestamp);
  }

  public int hashCode() {
    return user.hashCode() * 100 + timestamp.hashCode();
  }
}

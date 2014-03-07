package com.inmobi.conduit.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestMsgSizeOfAllStreams {

  public void testMsgSizes(String hdfsUri) throws IOException {
    Path hdfsUrl = new Path(hdfsUri);
    FileSystem fs = hdfsUrl.getFileSystem(new Configuration());
    Path dataDir = new Path(hdfsUrl, "databus/system/checkpoint");
    FileStatus[] checkpointFiles = fs.listStatus(dataDir);
    if (checkpointFiles != null) {
      System.out.println("Number of checkpoints " + checkpointFiles.length); 
    } else {
      System.out.println("No checkpoints in checkpoint dir ");
     return;
    }
    for (FileStatus chkFile : checkpointFiles) {
      System.out.println(" Checkpoint file " + chkFile.getPath());
      FSDataInputStream fin = null;
      BufferedReader breader = null;
      try {
        fin = fs.open(chkFile.getPath());
        breader = new BufferedReader(new InputStreamReader(fin));
        String msg = breader.readLine();
        if (msg != null) {
          System.out.println(" Service " + chkFile.getPath().getName() + " checkpoint is " + msg);
        }
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        if (fin != null) {
          fin.close();
        }
        if (breader != null) {
          breader.close(); 
        }
      }
    }
  }

  public static void main(String[] args) throws IOException {
    TestMsgSizeOfAllStreams obj = new TestMsgSizeOfAllStreams();
    obj.testMsgSizes(args[0]);
  }
}

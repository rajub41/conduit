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
    Path dataDir = new Path(hdfsUrl, "databus/data");
    FileStatus[] streams = fs.listStatus(dataDir);
    if (streams != null) {
   System.out.println("NNNNNNNNNNNNNNNNumber of streams " + streams.length);
   
    } else {
      System.out.println("NNNNNNNNNNNNNNNNNNO streams ");
     return;
    }
    for (FileStatus streamSt : streams) {
      System.out.println(" Stream Name   ===========================   msgLength ");
      FileStatus[] collectorStatuses = fs.listStatus(streamSt.getPath());
      if (collectorStatuses == null) {
        System.out.println("NNNNNNNNNooooo collectors in " + streamSt.getPath());
        break;
      }
      for (FileStatus collectorSt : collectorStatuses) {
        
        FileStatus[] files = fs.listStatus(collectorSt.getPath());
         if (files == null) {
           System.out.println("No files in that collector " + collectorSt.getPath());
           break;
         }
         
         // for each file
         for (FileStatus file: files) {
           if (file.getLen() == 0) {
             System.out.println("reading from other file as this file " + file.getPath() +" is empty");
             continue;
           }
           FSDataInputStream fin = null;
           BufferedReader breader = null;
           try {
             fin = fs.open(file.getPath());
             breader = new BufferedReader(new InputStreamReader(fin));
             String msg = breader.readLine();
             if (msg != null) {
               System.out.println(file.getPath().getParent().getParent().getName() + "                       "+  msg.length());
             }
           } catch (Exception e) {
             // TODO Auto-generated catch block
             e.printStackTrace();
           } finally {
              if (fin != null) {
                fin.close();
              }
              if (breader != null) {
               breader.close(); 
              }
           }
           break;
         }
         
         break;
      }
    }
  }

  public static void main(String[] args) throws IOException {
    TestMsgSizeOfAllStreams obj = new TestMsgSizeOfAllStreams();
    obj.testMsgSizes(args[0]);
  }
}

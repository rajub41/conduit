package com.inmobi.conduit.utils;

/*
 * #%L
 * Conduit Worker
 * %%
 * Copyright (C) 2012 - 2014 InMobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import com.inmobi.conduit.Cluster;

/**
 * This class finds the out of order minute directories in various streams 
 * for different clusters. 
 * This class main function takes list of root dirs, base dirs, stream names
 *  as arguments. All are comma separated 
 */
public class OrderlyCreationOfDirs {
  private static final Log LOG = LogFactory.getLog(
      OrderlyCreationOfDirs.class);
  
  private static final int DATE_FORMAT_LENGTH = CalendarHelper.minDirFormatStr.
      length();

  public OrderlyCreationOfDirs() {
  }

  /**
   * This method lists all the minute directories for a particular 
   * stream category.
   */
  public void doRecursiveListing(Path dir, Set<Path> listing, 
      FileSystem fs, int streamDirlen) throws IOException {
    FileStatus[] fileStatuses = null;
    try {
      fileStatuses = fs.listStatus(dir);
    } catch (FileNotFoundException e) {

    }
    if (fileStatuses == null || fileStatuses.length == 0) {
      LOG.debug("No files in directory:" + dir);
      if (dir.toString().length() == (streamDirlen + DATE_FORMAT_LENGTH + 1)) {
        listing.add(dir);
      } 
    } else {
      for (FileStatus file : fileStatuses) {  
        if (file.isDir()) {
          doRecursiveListing(file.getPath(), listing, fs, streamDirlen);   
        } else {
          listing.add(file.getPath().getParent());
        }       
      } 
    }
  }

  /**
   *  This method finds the out of order minute directories for a 
   *  particular stream.
   *  @param creationTimeOfFiles : TreeMap for all the directories statuses
   *   for a particular stream
   *  @param outOfOrderDirs : store out of order directories : outOfOrderDirs
   */
  public void validateOrderlyCreationOfPaths(Path streamDir,
      TreeMap<Date, FileStatus> creationTimeOfFiles, List<Path> outOfOrderDirs,
      Set<Path> notCreatedMinutePaths) {
    Date previousKeyEntry = null;
    for (Date presentKeyEntry : creationTimeOfFiles.keySet() ) {
      if (previousKeyEntry != null) {
        if (creationTimeOfFiles.get(previousKeyEntry).getModificationTime()
            > creationTimeOfFiles.get(presentKeyEntry).getModificationTime()) {
          System.out.println("Directory is created in out of order :    " + 
              creationTimeOfFiles.get(presentKeyEntry).getPath()); 
          outOfOrderDirs.add(creationTimeOfFiles.get(previousKeyEntry)
              .getPath());
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(previousKeyEntry);
        calendar.add(Calendar.MINUTE, 1);
        while (presentKeyEntry.compareTo(calendar.getTime()) != 0) {
          Path parentPath, missingPath;
          missingPath = new Path(streamDir,
              Cluster.getDateAsYYYYMMDDHHMNPath(calendar.getTime()));
          System.out.println("Missing Dir: " + missingPath);
          notCreatedMinutePaths.add(missingPath);
          calendar.add(Calendar.MINUTE, 1);
        }
      }
      previousKeyEntry = presentKeyEntry;
    }
  }

  public void listingAndValidation(Path streamDir, FileSystem fs,
      List<Path> outOfOrderDirs, Set<Path> notCreatedMinutePaths)
          throws IOException {
    Set<Path> listing = new HashSet<Path>();
    TreeMap<Date, FileStatus>creationTimeOfFiles = new TreeMap<Date, 
        FileStatus >();
    // passing stream dir length as argument to know the minute dir
    doRecursiveListing(streamDir, listing, fs, streamDir.toString().length());
    for (Path path :listing) {
      creationTimeOfFiles.put(CalendarHelper.getDateFromStreamDir(
          streamDir, path), fs.getFileStatus(path));
    }
    validateOrderlyCreationOfPaths(streamDir, creationTimeOfFiles,
        outOfOrderDirs, notCreatedMinutePaths);
  }

  public void getStreamNames(String baseDir, String rootDir, List<String>
  streamNames) throws Exception {
    FileSystem baseDirFs = new Path(rootDir, baseDir).getFileSystem
        (new Configuration());
    FileStatus[] streamFileStatuses;
    try {
      streamFileStatuses = baseDirFs.listStatus(new Path
        (rootDir, baseDir));
    } catch (FileNotFoundException e) {
      streamFileStatuses = new FileStatus[0];
    }
    if (streamFileStatuses != null) {
      for (FileStatus file : streamFileStatuses) {
        if (!streamNames.contains(file.getPath().getName())) {
          streamNames.add(file.getPath().getName());
        }
      }  
    }
  }

  public void getBaseDirs(String baseDirArg, List<String> baseDirs) {
    for (String baseDir : baseDirArg.split(",")) {
      baseDirs.add(baseDir);
    }
  }

  public List<Path> run(String [] args) throws Exception {
    List<Path> outoforderdirs = new ArrayList<Path>();
    Set<Path> notCreatedMinutePaths = new HashSet<Path>();
    String[]	rootDirs = args[0].split(",");
    List<String> baseDirs = new ArrayList<String>();
    List<String> streamNames;
    if (args.length == 1) {
      baseDirs.add("streams");
      baseDirs.add("streams_local");
      for (String rootDir : rootDirs) {
        for (String baseDir : baseDirs) {
          streamNames = new ArrayList<String>();
          getStreamNames(baseDir, rootDir, streamNames);
          pathConstruction(rootDir, baseDir, streamNames, outoforderdirs,
              notCreatedMinutePaths);
        }
      }
      if (outoforderdirs.isEmpty()) {
        System.out.println("There are no out of order dirs");
      }
      if (notCreatedMinutePaths.isEmpty()) {
        System.out.println("There are no missing dirs");
      }
    } else if (args.length == 2) {
      getBaseDirs(args[1], baseDirs);
      for (String rootDir : rootDirs) {
        for (String baseDir : baseDirs) {
          streamNames = new ArrayList<String>();
          getStreamNames(baseDir, rootDir, streamNames);
          pathConstruction(rootDir, baseDir, streamNames, outoforderdirs,
              notCreatedMinutePaths);
        }
      }
      if (outoforderdirs.isEmpty()) {
        System.out.println("There are no out of order dirs");
      }
      if (notCreatedMinutePaths.isEmpty()) {
        System.out.println("There are no missing dirs");
      }
    } else if (args.length == 3) {
      getBaseDirs(args[1], baseDirs);
      streamNames = new ArrayList<String>();
      for (String streamname : args[2].split(",")) {
        streamNames.add(streamname);
      }
      for (String rootDir : rootDirs) {
        for (String baseDir : baseDirs) {
          pathConstruction(rootDir, baseDir, streamNames, outoforderdirs,
              notCreatedMinutePaths);
        }
      }
      if (outoforderdirs.isEmpty()) {
        System.out.println("There are no out of order dirs");
      } 
      if (notCreatedMinutePaths.isEmpty()) {
        System.out.println("There are no missing dirs");
      }
    }
    return outoforderdirs;
  }

  /**
   * @param  rootDir : array of root directories
   * @param  baseDir : array of baseDirs
   * @param  streamNames : array of stream names
   * @return outOfOrderDirs: list of out of directories for all the streams.
   */
  public void pathConstruction(String rootDir, String baseDir,
      List<String> streamNames, List<Path> outOfOrderDirs,
      Set<Path> notCreatedMinutePaths) throws IOException {
    FileSystem fs = new Path(rootDir).getFileSystem(new Configuration());
    Path rootBaseDirPath = new Path(rootDir, baseDir);
    for (String streamName : streamNames) {
      Path streamDir = new Path(rootBaseDirPath , streamName);
      FileStatus[] files = null;
      try {
        files = fs.listStatus(streamDir);
      } catch (FileNotFoundException e) {

      }
      if (files == null || files.length == 0) {
        LOG.info("No direcotries in that stream: " + streamName);
        continue;
      }
      listingAndValidation(streamDir, fs, outOfOrderDirs, notCreatedMinutePaths);
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length >= 1) {
      OrderlyCreationOfDirs obj = new OrderlyCreationOfDirs();
      obj.run(args); 
    } else {
      System.out.println("Insufficient number of arguments: 1st argument:" +
          " rootdirs," + " 2nd arument :basedirs, 3rd arguments: streamnames"
          + " 2nd arg, 3rd args are optionl here");
      System.exit(1);
    }
  }
}

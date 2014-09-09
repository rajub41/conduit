/*
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
 */
package com.inmobi.conduit.distcp;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.inmobi.conduit.AbstractService;
import com.inmobi.conduit.CheckpointProvider;
import com.inmobi.conduit.Cluster;
import com.inmobi.conduit.Conduit;
import com.inmobi.conduit.ConduitConfig;
import com.inmobi.conduit.DestinationStream;
import com.inmobi.conduit.HCatClientUtil;
import com.inmobi.conduit.utils.CalendarHelper;
import com.inmobi.conduit.utils.DatePathComparator;
import com.inmobi.conduit.utils.FileUtil;
import com.inmobi.conduit.utils.HCatPartitionComparator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hive.hcatalog.api.HCatAddPartitionDesc;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatPartition;
import org.apache.hive.hcatalog.common.HCatException;

import com.google.common.collect.Table;
import com.inmobi.audit.thrift.AuditMessage;
import com.inmobi.conduit.metrics.ConduitMetrics;


/*
 * Handles MergedStreams for a Cluster
 */

public class MergedStreamService extends DistcpBaseService {

  private static final Log LOG = LogFactory.getLog(MergedStreamService.class);
  
  public static final Map<String, Long> lastAddedPartitionMap = new HashMap<String, Long>();
  public static final Map<String, Boolean> streamHcatEnableMap = new HashMap<String, Boolean>();
  protected static boolean failedTogetPartitions = false;

  private void prepareStreamHcatEnableMap() {
    Map<String, DestinationStream> destStreamMap = destCluster.getDestinationStreams();
    for (String stream : streamsToProcess) {
      if (destStreamMap.containsKey(stream)
          && destStreamMap.get(stream).isHCatEnabled()) {
        streamHcatEnableMap.put(stream, true);
      } else {
        streamHcatEnableMap.put(stream, false);
      }
    }
  }

  public void prepareLastAddedPartitionMap() throws InterruptedException {
    // TODO re-factor this method name if required
    prepareStreamHcatEnableMap();

    HCatClient hcatClient = getHCatClient();
    if (hcatClient == null) {
      return;
    }

    for (String stream : streamsToProcess) {
      if (streamHcatEnableMap.get(stream)) {
        try {
          // TODO rename if required
          findLastPartition(hcatClient, stream);
        } catch (HCatException e) {
          LOG.warn("Got Exception while finding hte last added partition for"
              + " each stream");
          failedTogetPartitions = true;
          e.printStackTrace();
        }
      } else {
        LOG.info("Hcatalog is not enabled for " + stream + " stream");
      }
    }
    submitBack(hcatClient);
  }

  protected void findLastPartition(HCatClient hcatClient, String stream)
      throws HCatException {
    List<HCatPartition> hCatPartitionList = hcatClient.getPartitions(
        Conduit.getHcatDBName(), getTableName(stream));
    if (hCatPartitionList.isEmpty()) {
      LOG.info("No partitions present for " + stream + " stream ");
      lastAddedPartitionMap.put(stream, (long) -1);
      return;
    }
    Collections.sort(hCatPartitionList, new HCatPartitionComparator());
    HCatPartition lastHcatPartition = hCatPartitionList.get(hCatPartitionList.size()-1);
    Date lastAddedPartitionDate = getTimeStampFromHCatPartition(
        lastHcatPartition.getLocation(), stream);
    if (lastAddedPartitionDate != null) {
      LOG.info("AAAAAAAAAAAAAAAAAAAAAAAAAAAAA find last added partition : " + lastAddedPartitionDate.getTime());
      lastAddedPartitionMap.put(stream, lastAddedPartitionDate.getTime());
    } else {
      // if there are no partitions in the hcatalog table then it should create partitions from current time
      LOG.info("AAAAAAAAAAAAAAAAAAAAAAAAAAAAA find last added partition : " + (-1));
      lastAddedPartitionMap.put(stream, (long) -1);
    }
  }

  public MergedStreamService(ConduitConfig config, Cluster srcCluster,
      Cluster destinationCluster, Cluster currentCluster,
      CheckpointProvider provider, Set<String> streamsToProcess,
      HCatClientUtil hcatUtil) throws Exception {
    super(config, "MergedStreamService_" + getServiceName(streamsToProcess),
        srcCluster, destinationCluster, currentCluster, provider,
        streamsToProcess, hcatUtil);

    for (String eachStream : streamsToProcess) {
      ConduitMetrics.registerSlidingWindowGauge(getServiceType(), AbstractService.RETRY_CHECKPOINT, eachStream);
      ConduitMetrics.registerSlidingWindowGauge(getServiceType(), AbstractService.RETRY_MKDIR, eachStream);
      ConduitMetrics.registerSlidingWindowGauge(getServiceType(), AbstractService.RETRY_RENAME, eachStream);
      ConduitMetrics.registerSlidingWindowGauge(getServiceType(), AbstractService.EMPTYDIR_CREATE, eachStream);
      ConduitMetrics.registerSlidingWindowGauge(getServiceType(), AbstractService.FILES_COPIED_COUNT, eachStream);
      ConduitMetrics.registerSlidingWindowGauge(getServiceType(), RUNTIME, eachStream);
      ConduitMetrics.registerSlidingWindowGauge(getServiceType(), FAILURES,
          eachStream);
      ConduitMetrics.registerSlidingWindowGauge(getServiceType(),
          COMMIT_TIME, eachStream);
      ConduitMetrics.registerAbsoluteGauge(getServiceType(),
          LAST_FILE_PROCESSED, eachStream);
    }
  }

  @Override
  protected Path getDistCpTargetPath() {
    return new Path(getDestCluster().getTmpPath(),
        "distcp_mergedStream_" + getSrcCluster().getName() + "_"
            + getDestCluster().getName() + "_"
            + AbstractService.getServiceName(streamsToProcess)).makeQualified(getDestFs());
  }

  @Override
  public void execute() throws Exception {
    lastProcessedFile.clear();
    List<AuditMessage> auditMsgList = new ArrayList<AuditMessage>();
    LOG.info("Starting a run of service " + getName());
    Path tmpOut = getDistCpTargetPath();
    try {
      boolean skipCommit = false;

      // CleanuptmpOut before every run
      // the dest is a misnomer here. The service runs on the destination, so
      //all the meaning of dest is actually local cluster.

      if (getDestFs().exists(tmpOut))
        getDestFs().delete(tmpOut, true);
      if (!getDestFs().mkdirs(tmpOut)) {
        LOG.warn("Cannot create [" + tmpOut + "]..skipping this run");
        return;
      }

      synchronized (getDestCluster()) {
        long commitTime = getDestCluster().getCommitTime();
        publishMissingPaths(commitTime, streamsToProcess);
      }

      Map<String, FileStatus> fileListingMap = getDistCPInputFile();
      if (fileListingMap.size() == 0) {
        LOG.warn("No data to pull from " + "Cluster ["
            + getSrcCluster().getReadUrl() + "]" + " to Cluster ["
            + getDestCluster().getHdfsUrl() + "]");
        finalizeCheckPoints();
        for (String eachStream : streamsToProcess) {
          if (lastProcessedFile.get(eachStream) != null) {
            ConduitMetrics.updateAbsoluteGauge(getServiceType(),
                LAST_FILE_PROCESSED, eachStream, lastProcessedFile.get(eachStream));
          }
        }
        return;
      }
      LOG.info("Starting a distcp pull from Cluster ["
          + getSrcCluster().getReadUrl() + "]" + " to Cluster ["
          + getDestCluster().getHdfsUrl() + "] " + " Path ["
          + tmpOut.toString() + "]");

      try {
        if (!executeDistCp(getName(), fileListingMap, tmpOut))
          skipCommit = true;
      } catch (Throwable e) {
        LOG.warn("Error in distcp", e);
        LOG.warn("Problem in MergedStream distcp PULL..skipping commit for this run");
        throw new Exception(e);
      }
      Map<Path, Path> commitPaths = new HashMap<Path, Path>();
      // if success
      if (!skipCommit) {
        Map<String, List<Path>> categoriesToCommit = prepareForCommit(tmpOut);
        FileSystem fs = getDestFs();
        Table<String, Long, Long> parsedCounters = parseCountersFile(fs);
        synchronized (getDestCluster()) {
          long commitTime = getDestCluster().getCommitTime();
          // between the last addPublishMissinPaths and this call,distcp is
          // called which is a MR job and can take time hence this call ensures
          // all missing paths are added till this time
          publishMissingPaths(commitTime, streamsToProcess);
          commitPaths = createLocalCommitPaths(tmpOut, commitTime,
              categoriesToCommit);
          // category, Set of Paths to commit
          doLocalCommit(commitPaths, auditMsgList, parsedCounters);
        }
        finalizeCheckPoints();
        for (String eachStream : streamsToProcess) {
          if (lastProcessedFile.get(eachStream) != null) {
            ConduitMetrics.updateAbsoluteGauge(getServiceType(),
                LAST_FILE_PROCESSED, eachStream, lastProcessedFile.get(eachStream));
          }
        }
      }
    } catch (Exception e) {
      LOG.warn("Error in run ", e);
      throw e;
    } finally {
      // rmv tmpOut cleanup
      getDestFs().delete(tmpOut, true);
      LOG.debug("Deleting [" + tmpOut + "]");
      publishAuditMessages(auditMsgList);
    }
  }

  public void publishPartitions(long commitTime, String streamName)
      throws InterruptedException {
    if (!streamHcatEnableMap.containsKey(streamName)
        || !streamHcatEnableMap.get(streamName)) {
      LOG.info("Hcat is not enabled for " + streamName + " stream");
      return;
    }
    HCatClient hcatClient = getHCatClient();
    if (hcatClient == null) {
      return;
    }
    try {
      long lastAddedTime = lastAddedPartitionMap.get(streamName);
      LOG.info("AAAAAAAAAAAAAAAAAAA lastadded partition time : " + lastAddedTime);
      if (lastAddedTime == -1) {
        if (!failedTogetPartitions) {
          lastAddedPartitionMap.put(streamName, commitTime - MILLISECONDS_IN_MINUTE);
          LOG.info("AAAAAAAAAAAAAAAAAAA lastadded partition time : not failed  " + lastAddedTime);
          return;
        } else {
          // TODO 
          try {
            findLastPartition(hcatClient, streamName);
            lastAddedTime = lastAddedPartitionMap.get(streamName);
            if (lastAddedTime == -1) {
              lastAddedPartitionMap.put(streamName, commitTime - MILLISECONDS_IN_MINUTE);
              LOG.info("AAAAAAAAAAAAAAAAAAA lastadded partition time  failed : " + lastAddedTime);

              return;
            }
          } catch (HCatException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return;
          }
        }
      }
      long nextPartitionTime = lastAddedTime + MILLISECONDS_IN_MINUTE;
      if (isMissingPartitions(commitTime, nextPartitionTime)) {
        LOG.info("Last added partition : [" + getLogDateString(lastAddedTime) + "]");
        while (isMissingPartitions(commitTime, nextPartitionTime)) {
          String missingPartition = Cluster.getDestDir(
              srcCluster.getFinalDestDirRoot(), streamName, nextPartitionTime);
          try {
            LOG.info("AAAAAAAAAAAAAAAAAAAAAAAAAA misssing partition : " + missingPartition);
            if (addPartition(missingPartition, streamName, nextPartitionTime,
                getTableName(streamName), hcatClient)) {
              lastAddedPartitionMap.put(streamName, nextPartitionTime);
            } else {
              LOG.error("Exception occured while trying to add partition ");
              break;
            }
            nextPartitionTime = nextPartitionTime + MILLISECONDS_IN_MINUTE;
          } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            break;
          }
        }
      }
    } catch (Exception e) {

      // TODO Auto-generated catch block
      e.printStackTrace();
    } finally {
      submitBack(hcatClient);
    }
  }

  private void publishMissingPaths(long commitTime,
      Set<String> categoriesToCommit) throws Exception {
    publishMissingPaths(getDestFs(),
        getDestCluster().getFinalDestDirRoot(), commitTime, categoriesToCommit);
  }

  private Map<String, List<Path>> prepareForCommit(Path tmpOut)
      throws Exception {
    Map<String, List<Path>> categoriesToCommit = new HashMap<String, List<Path>>();
    FileStatus[] allFilesPerStream = null;
    Path tmpPathPerStream = null;
    for(String stream:streamsToProcess){
      tmpPathPerStream = new Path(tmpOut,stream);
      try {
        allFilesPerStream = FileUtil.listStatusAsPerHDFS(getDestFs(),
            tmpPathPerStream);
      } catch (FileNotFoundException ignored) {
      }

      if (allFilesPerStream != null) {
        for (FileStatus fileStatus : allFilesPerStream) {
          List<Path> fileList = categoriesToCommit.get(stream);
          if (fileList == null) {
            fileList = new ArrayList<Path>();
            fileList.add(fileStatus.getPath());
            categoriesToCommit.put(stream, fileList);
          } else {
            fileList.add(fileStatus.getPath());
          }
        }
      }
    }
    return categoriesToCommit;
  }

  /*
   * @returns Map<Path, Path> - Map of filePath, destinationPath committed for
   * stream destinationPath :
   * hdfsUrl/<rootdir>/streams/<category>/YYYY/MM/HH/MN/filename
   */
  public Map<Path, Path> createLocalCommitPaths(Path tmpOut, long commitTime,
      Map<String, List<Path>> categoriesToCommit) throws Exception {

    // find final destination paths
    Map<Path, Path> mvPaths = new LinkedHashMap<Path, Path>();
    Set<Map.Entry<String, List<Path>>> commitEntries = categoriesToCommit
        .entrySet();
    Iterator<Entry<String, List<Path>>> it = commitEntries.iterator();
    while (it.hasNext()) {
      Map.Entry<String, List<Path>> entry = (Map.Entry<String, List<Path>>) it
          .next();
      String category = entry.getKey();
      List<Path> filesInCategory = entry.getValue();
      for (Path filePath : filesInCategory) {
        Path destParentPath = new Path(getDestCluster().getFinalDestDir(
            category, commitTime));
        Path commitPath = new Path(destParentPath, filePath.getName());
        mvPaths.put(filePath, commitPath);
      }
    }
    return mvPaths;
  }

  private void doLocalCommit(Map<Path, Path> commitPaths,
      List<AuditMessage> auditMsgList, Table<String, Long, Long> parsedCounters)
          throws Exception {
    LOG.info("Committing " + commitPaths.size() + " paths.");
    FileSystem fs = getDestFs();
    long startTime = System.currentTimeMillis();
    for (Map.Entry<Path, Path> entry : commitPaths.entrySet()) {
      LOG.info("Renaming " + entry.getKey() + " to " + entry.getValue());
      String streamName = getTopicNameFromDestnPath(entry.getValue());
      retriableMkDirs(fs, entry.getValue().getParent(), streamName);
      if (retriableRename(fs, entry.getKey(), entry.getValue(), streamName) == false) {
        LOG.warn("Rename failed, aborting transaction COMMIT to avoid "
            + "dataloss. Partial data replay could happen in next run");
        throw new Exception("Abort transaction Commit. Rename failed from ["
            + entry.getKey() + "] to [" + entry.getValue() + "]");
      }
      String filename = entry.getKey().getName();
      generateAuditMsgs(streamName, filename, parsedCounters, auditMsgList);
      ConduitMetrics.updateSWGuage(getServiceType(), AbstractService.FILES_COPIED_COUNT,
          streamName, 1);
    }
    long elapsedTime = System.currentTimeMillis() - startTime;
    LOG.debug("Committed " + commitPaths.size() + " paths.");
    for (String eachStream : streamsToProcess) {
      ConduitMetrics.updateSWGuage(getServiceType(), COMMIT_TIME,
          eachStream, elapsedTime);
    }

  }

  protected Path getInputPath() throws IOException {
    String finalDestDir = getSrcCluster().getReadLocalFinalDestDirRoot();
    return new Path(finalDestDir);

  }

  private boolean isValidYYMMDDHHMMPath(Path prefix, Path path) {
    if (path.depth() < prefix.depth() + 5)
      return false;
    return true;
  }

  private void filterInvalidPaths(List<FileStatus> listOfFileStatus, Path prefix) {
    Iterator<FileStatus> iterator = listOfFileStatus.iterator();
    while (iterator.hasNext()) {
      if (!isValidYYMMDDHHMMPath(prefix, iterator.next().getPath()))
        iterator.remove();
    }
  }

  /**
   * This method would first try to find the starting directory by comparing the
   * destination files with source files. For eg: If a file
   * /streams/2013/04/22/16/40/a.gz is present on destination than a.gz would be
   * searched in the all the folders of source. One minute would be added to the
   * found path and would be returned. If we cannot compute the starting
   * directory by this way than the first path on the source would be returned.
   * Also during migration from V1 to V2 there is a possibility that some paths
   * of the last minute directory found at source may still not be copied (due
   * to ytm file) hence returning the uncopied path of last directory as well
   * 
   * @throws IOException
   */
  @Override
  protected Path getStartingDirectory(String stream,
      List<FileStatus> filesToBeCopied) throws IOException {
    LOG.info("Finding starting directory for merge stream from SrcCluster "
        + srcCluster.getName() + " to Destination cluster "
        + destCluster.getName() + " for stream " + stream);
    Path pathToBeListed = new Path(destCluster.getFinalDestDirRoot(), stream);
    List<FileStatus> destnFiles = null;
    try {
      if (getDestFs().exists(pathToBeListed)) {
        // TODO decide between removing invalid paths after recursive ls or
        // while ls
        destnFiles = recursiveListingOfDir(getDestFs(), pathToBeListed);
        filterInvalidPaths(destnFiles, pathToBeListed);
        Collections.sort(destnFiles, new DatePathComparator());
        LOG.debug("File found on destination after sorting for stream" + stream
            + " are " + FileUtil.toStringOfFileStatus(destnFiles));
      }
    } catch (IOException e) {
      LOG.error("Error while listing path" + pathToBeListed
          + " on destination Fs");
    }
    Path lastLocalPathOnSrc = null;
    pathToBeListed = new Path(srcCluster.getReadLocalFinalDestDirRoot(), stream);
    List<FileStatus> sourceFiles = null;
    try {
      if (getSrcFs().exists(pathToBeListed)) {
        sourceFiles = recursiveListingOfDir(getSrcFs(), pathToBeListed);
        filterInvalidPaths(sourceFiles, pathToBeListed);
        Collections.sort(sourceFiles, new DatePathComparator());
        LOG.debug("File found on source after sorting for stream" + stream
            + " are " + FileUtil.toStringOfFileStatus(sourceFiles));
      }
    } catch (IOException e) {
      LOG.error("Error while listing path" + pathToBeListed + " on source Fs");
    }

    if (destnFiles != null && sourceFiles != null) {
      for (int i = destnFiles.size() - 1; i >= 0; i--) {
        FileStatus current = destnFiles.get(i);
        if (current.isDir())
          continue;
        lastLocalPathOnSrc = searchFileInSource(current, sourceFiles);
        if (lastLocalPathOnSrc != null) {
          break;
        }
      }
    }
    if (lastLocalPathOnSrc == null) {
      /*
       * We cannot figure out the last processed local path because either
       * 1)There were no files for this stream on destination 2) None of the
       * files on destination was found on source cluster In both these cases
       * checkpointing the starting of the stream and if there are no source
       * files than checkpoint the current time
       */
      if (sourceFiles != null && sourceFiles.size() != 0) {
        FileStatus firstPath = sourceFiles.get(0);
        if (firstPath.isDir())
          lastLocalPathOnSrc = firstPath.getPath();
        else
          lastLocalPathOnSrc = firstPath.getPath().getParent();
        LOG.info("Starting directory couldn't be figured out hence returning the"
            + " first path at the source");
      } else {
        LOG.info("No start directory can be computed for stream " + stream);
        return null;
      }

    } else {
      // Starting path was computed from source files
      // checking whether last directory was completely copied
      FileStatus[] filesInLastDir = FileUtil.listStatusAsPerHDFS(getSrcFs(),
          lastLocalPathOnSrc);
      for (FileStatus fileStatus : filesInLastDir) {
        if (searchFileInSource(fileStatus, destnFiles) == null) {
          // file present in source but absent in destination
          filesToBeCopied.add(fileStatus);
        }
      }
      // adding one minute to the found path to get the starting directory
      Path streamLevelLocalDir = new Path(getSrcCluster()
          .getReadLocalFinalDestDirRoot() + stream);
      Date date = CalendarHelper.getDateFromStreamDir(streamLevelLocalDir,
          lastLocalPathOnSrc);
      lastLocalPathOnSrc = CalendarHelper.getNextMinutePathFromDate(date,
          streamLevelLocalDir);

    }
    LOG.info("The start value is " + lastLocalPathOnSrc + " for stream "
        + stream);
    return lastLocalPathOnSrc;
  }

  /*
   * This method would search just the last part of the file in the source
   * cluster file .Expects the list to be sorted
   */
  private Path searchFileInSource(FileStatus destnPath,
      List<FileStatus> srcFiles) {
    LOG.debug("Searching path " + destnPath.getPath().toString());
    for (int i = srcFiles.size() - 1; i >= 0; i--) {
      FileStatus current = srcFiles.get(i);
      if (current.isDir())
        continue;
      if (current.getPath().getName().equals(destnPath.getPath().getName())) {
        FileStatus result = srcFiles.get(i);
        LOG.debug("Path found at " + result.getPath());
        if (!result.isDir())
          return result.getPath().getParent();
        else
          return result.getPath();
      }
    }
    LOG.debug("Path not found " + destnPath.getPath());
    return null;

  }

  private List<FileStatus> recursiveListingOfDir(FileSystem currentFs, Path path) {

    try {
      FileStatus streamDir = currentFs.getFileStatus(path);
      List<FileStatus> filestatus = new ArrayList<FileStatus>();
      createListing(currentFs, streamDir, filestatus);
      return filestatus;
    } catch (IOException ie) {
      LOG.error(
          "IOException while doing recursive listing to create checkpoint on " +
              "cluster filesystem" + currentFs.getUri(), ie);
    }
    return null;

  }

  /*
   * srcPath is the FileStatus of minute directory in streams_local directory
   * eg: /conduit/streams_local/<streamName>/2013/09/28/15/04
   */
  @Override
  protected String getFinalDestinationPath(FileStatus srcPath) {
    if (srcPath.isDir())
      return null;
    else {
      String streamName = srcPath.getPath().getParent().getParent().getParent()
          .getParent().getParent().getParent().getName();
      return File.separator + streamName + File.separator
          + srcPath.getPath().getName();
    }

  }

  @Override
  protected String getTier() {
    return "MERGE";
  }

  public String getServiceType() {
    return "MergedStreamService";
  }
}

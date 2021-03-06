package com.inmobi.conduit.utils;

import java.util.ArrayList;
import java.util.List;

import com.inmobi.conduit.Cluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestMergeStreamDataConsistency {
	private static Logger LOG = Logger.getLogger(
			TestMergeStreamDataConsistency.class);
	private static String className = TestMergeStreamDataConsistency.class
			.getSimpleName();
	FileSystem fs;
	String mergedStreamUrl = "file:///tmp/test/" + className + "/1/" ;
	String localStreamUrl = ("file:///tmp/test/" + className + "/2/," +
			"file:///tmp/test/" + className +"/3/");
	String [] localStreamUrls = localStreamUrl.split(",");

	List<String> emptyStreamName = new ArrayList<String>();
	List<String> missedFilesStreamName = new ArrayList<String>();
	List<String> emptyDirStreamName = new ArrayList<String>();
	List<String> consistentDataStreamName = new ArrayList<String>();
	List<String> dataReplayFilesStreamName = new ArrayList<String>();
	List<String> extrafilesStreamName = new ArrayList<String>();
	List<String> singleFileStreamName = new ArrayList<String>();
	List<String> missingSingleFileStreamName = new ArrayList<String>();
	List<String> inconsistencyAtEndOfStream = new ArrayList<String>();
	List<String> purgedPathStreaName = new ArrayList<String>();
	List<String> purgedOnLocalStream = new ArrayList<String>();
	List<String> duplicateFileStream = new ArrayList<String>();
	List<String> allStreamNames = new ArrayList<String>();

	List<Path> emptyPaths = new ArrayList<Path>();
	List<Path> emptyDirsPaths = new ArrayList<Path>();
	List<Path> missedFilePaths = new ArrayList<Path>();
	List<Path> dataReplayFilePaths = new ArrayList<Path>();
	List<Path> extraFilePaths = new ArrayList<Path>();
	List<Path> missingSingleFilePath =  new ArrayList<Path>();
	List<Path> inconsistencyEndPaths = new ArrayList<Path>();
	// It stores all purged paths on the streams 
	List<Path> purgedPaths = new ArrayList<Path>();
	// It stores all the purged paths on the streams_local
	List<Path> purgedLocalPaths = new ArrayList<Path>();
	List<Path> duplicateFilePaths = new ArrayList<Path>();
	
	boolean missing = false;
	boolean missingAtEnd = false;
	boolean purgedFlag = false;
	boolean purgedLocalFlag = false;
	long temptime = System.currentTimeMillis();

	@BeforeTest
	public void setup() throws Exception {
		fs = FileSystem.getLocal(new Configuration());
		// clean up the test data if any thing is left in the previous runs
		cleanup();
		defineStreamNames(emptyStreamName, "empty");
		defineStreamNames(emptyDirStreamName, "emptyDirs");
		defineStreamNames(consistentDataStreamName, "consistentData");
		defineStreamNames(missedFilesStreamName, "missingFiles");
		defineStreamNames(dataReplayFilesStreamName, "dataReplayFiles");
		defineStreamNames(extrafilesStreamName, "extraFiles");
		defineStreamNames(singleFileStreamName, "singleFile");
		defineStreamNames(missingSingleFileStreamName, "missingSingleFile");
		defineStreamNames(inconsistencyAtEndOfStream, "inconsistencyAtEnd");
		defineStreamNames(purgedPathStreaName, "purgedFiles");
		defineStreamNames(purgedOnLocalStream, "purgeLocal");
		defineStreamNames(duplicateFileStream, "duplicates");
		defineStreamNames(allStreamNames, "empty");
		defineStreamNames(allStreamNames, "emptyDirs");
		defineStreamNames(allStreamNames, "consistentData");
		defineStreamNames(allStreamNames, "missingFiles");
		defineStreamNames(allStreamNames, "dataReplayFiles");
		defineStreamNames(allStreamNames, "extraFiles");
		defineStreamNames(allStreamNames, "singleFile");
		defineStreamNames(allStreamNames, "missingSingleFile");
		defineStreamNames(allStreamNames, "inconsistencyAtEnd");
		defineStreamNames(allStreamNames, "purgedFiles");
		defineStreamNames(allStreamNames, "purgeLocal");
		defineStreamNames(allStreamNames, "duplicates");
		
		createTestData(localStreamUrl, "local");
		createTestData(mergedStreamUrl, "merge");
	}

	@AfterTest
	public void cleanup() throws Exception {
		fs.delete(new Path(mergedStreamUrl).getParent(), true);
	}

	public void defineStreamNames(List<String> streamNames, String streamName) {
		streamNames.add(streamName);
	}
	public void createMinDirs(Path listPath, boolean streamFlag, int dirCount,
			List<String> streamNames, int start) throws Exception {
		int milliseconds = 60000;
		String date;
		Path streamDir;

		for (String streamName : streamNames) {
			for (int i = 1; i <= dirCount; i++) {
				streamDir = new Path(listPath, streamName);
				if (!streamFlag) {
					int numOfFiles = 5 * localStreamUrls.length;
					if (streamName.equals("emptyDirs")) {
						date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i *
                milliseconds);
						createFilesData(fs, new Path(streamDir, date), 0, 0);
					} else if (streamName.equals("consistentData")) {
						date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i *
								milliseconds);
						createFilesData(fs, new Path(streamDir, date), numOfFiles, 0);
					} else if (streamName.equals("missingFiles")) {
						date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i *
								milliseconds);
						createFilesData(fs, new Path(streamDir, date), 4, 0);
						createFilesData(fs, new Path(streamDir, date), 5, 5);
					} else if (streamName.equals("dataReplayFiles")) {
						date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i *
								milliseconds);
						createFilesData(fs, new Path(streamDir, date), numOfFiles + 2, 0);
						dataReplayFilePaths.add(new Path(new Path(streamDir, date),
								"file10"));
						dataReplayFilePaths.add(new Path(new Path(streamDir, date),
								"file11"));
					} else if (streamName.equals("extraFiles")) {
						date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i *
								milliseconds);
						createFilesData(fs, new Path(streamDir, date), numOfFiles, 0);
						extraFilePaths.add(new Path(new Path(streamDir, date), "file4"));
						extraFilePaths.add(new Path(new Path(streamDir, date), "file9"));
					} else if (streamName.equals("singleFile")) {
						date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i *
								milliseconds);
						createFilesData(fs, new Path(streamDir, date), 1, 0);
					} else if (streamName.equals("missingSingleFile")) {
						date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i *
								milliseconds);
						createFilesData(fs, new Path(streamDir, date), 1, 0);
						missingSingleFilePath.add(new Path(new Path(streamDir, date), "file0"));
					} else if (streamName.equals("inconsistencyAtEnd")) {
						date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i *
								milliseconds);
						createFilesData(fs, new Path(streamDir, date), numOfFiles - 1, 0);
						createFilesData(fs, new Path(streamDir, date), 2, 10);
						inconsistencyEndPaths.add(new Path(new Path(streamDir, date), "file10"));
						inconsistencyEndPaths.add(new Path(new Path(streamDir, date), "file11"));
					} else if (streamName.equals("purgedFiles")) {
					  date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i *
					      milliseconds);
					  createFilesData(fs, new Path(streamDir, date), numOfFiles - 3, start);  
					} else if (streamName.equals("purgeLocal")) {
					  date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i *
					      milliseconds);
					  createFilesData(fs, new Path(streamDir, date), numOfFiles, start);
					  purgedLocalPaths.add(new Path(new Path(streamDir, date), "file0"));
					  purgedLocalPaths.add(new Path(new Path(streamDir, date), "file1"));
					  purgedLocalPaths.add(new Path(new Path(streamDir, date), "file2"));
					} else if (streamName.equals("duplicates")) {
					  date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i *
					      milliseconds);
					  Path minDir = new Path(streamDir, date);
					  createFilesData(fs, minDir, numOfFiles, start);
					  if (i == 1) {
					    date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + (i + dirCount)
					        * milliseconds);
					    createFilesData(fs, new Path(streamDir, date), 1, 1);
					  }
					  duplicateFilePaths.add(new Path(minDir, "file1"));
					}
				} else {
					if (streamName.equals("emptyDirs")) {
						date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i *
								milliseconds);
						createFilesData(fs, new Path(streamDir, date), 0, 0);
					} else if (streamName.equals("consistentData")) {
						date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i *
								milliseconds);
						createFilesData(fs, new Path(streamDir, date), 5, start);
					} else if (streamName.equals("missingFiles")) {
						date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i *
								milliseconds);
						createFilesData(fs, new Path(streamDir, date), 5, start);
						if (!missing) {
							missedFilePaths.add(new Path(new Path(streamDir, date), "file4"));
						} 
						missing = true;
					} else if (streamName.equals("dataReplayFiles")) {
						date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i *
								milliseconds);
						createFilesData(fs, new Path(streamDir, date), 5, start);
					} else if (streamName.equals("extraFiles")) {
						date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i *
								milliseconds);
						createFilesData(fs, new Path(streamDir, date), 4, start);
					} else if (streamName.equals("singleFile")) {
						date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i *
								milliseconds);
						createFilesData(fs, new Path(streamDir, date), 1, start);
					} else if (streamName.equals("missingSingleFile")) {
						date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i *
								milliseconds);
						createFilesData(fs, new Path(streamDir, date), 1, start);
						missingSingleFilePath.add(new Path(new Path(streamDir, date), "file1"));
					} else if (streamName.equals("inconsistencyAtEnd")) {
						date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i *
								milliseconds);
						createFilesData(fs, new Path(streamDir, date), 5, start);
						if (missingAtEnd) {
							inconsistencyEndPaths.add(new Path(new Path(streamDir, date), "file9"));
						}
						missingAtEnd = true;
					} else if (streamName.equals("purgedFiles")) {
					  date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i *
					      milliseconds);
					  createFilesData(fs, new Path(streamDir, date), 5, start);
					  if (!purgedFlag) {
					    purgedPaths.add(new Path(new Path(streamDir, date), "file0"));
					    purgedPaths.add(new Path(new Path(streamDir, date), "file1"));
					    purgedPaths.add(new Path(new Path(streamDir, date), "file2"));
					    purgedFlag = true;
					  }
					} else if (streamName.equals("purgeLocal")) {
					  date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i *
					      milliseconds);
					  if (!purgedLocalFlag) {
					    createFilesData(fs, new Path(streamDir, date), 2, 3);
					    purgedLocalFlag = true;
					  } else {
					    createFilesData(fs, new Path(streamDir, date), 5, start);
					  }
					} else if (streamName.equals("duplicates")) {
					  date = Cluster.getDateAsYYYYMMDDHHMNPath(temptime + i *
					      milliseconds);
					  createFilesData(fs, new Path(streamDir, date), 5, start);
					}
				}	
			}	
		}
	}

	public void createTestData(String rootDir, String streamType) throws
			Exception {
		Path baseDir;
		if (streamType.equals("merge")) {
			int start = 0;
			baseDir = new Path(rootDir, "streams");
			createMinDirs(baseDir, false, 0, emptyStreamName, start);
			createMinDirs(baseDir, false, 1, emptyDirStreamName, start);
			createMinDirs(baseDir, false, 1, consistentDataStreamName, start);
			createMinDirs(baseDir, false, 1, missedFilesStreamName, start);
			createMinDirs(baseDir, false, 1, dataReplayFilesStreamName, start);
			createMinDirs(baseDir, false, 1, extrafilesStreamName, start);
			createMinDirs(baseDir, false, 1, singleFileStreamName, start);
			createMinDirs(baseDir, false, 1, missingSingleFileStreamName, start);
			createMinDirs(baseDir, false, 1, inconsistencyAtEndOfStream, start);
			createMinDirs(baseDir, false, 1, purgedPathStreaName, 3);
      createMinDirs(baseDir, false, 1, purgedOnLocalStream, start);
      createMinDirs(baseDir, false, 1, duplicateFileStream, start);
		} else {
			int start = 0;
			boolean fileCreated = false;
			
			for (String localStreamUrl : localStreamUrls) {
				baseDir = new Path(localStreamUrl, "streams_local");
				createMinDirs(baseDir, true, 0, emptyStreamName, start);
				createMinDirs(baseDir, true, 1, emptyDirStreamName, start);
				createMinDirs(baseDir, true, 1, consistentDataStreamName, start);
				createMinDirs(baseDir, true, 1, missedFilesStreamName, start);
				createMinDirs(baseDir, true, 1, dataReplayFilesStreamName, start);
				createMinDirs(baseDir, true, 1, extrafilesStreamName, start);
				if (!fileCreated) {
					createMinDirs(baseDir, true, 1, singleFileStreamName, start);
					createMinDirs(baseDir, true, 1, missingSingleFileStreamName, 1);
					fileCreated = true;
				}
				createMinDirs(baseDir, true, 1, inconsistencyAtEndOfStream, start);
				createMinDirs(baseDir, true, 1, purgedPathStreaName, start);
				createMinDirs(baseDir, true, 1, purgedOnLocalStream, start);
				createMinDirs(baseDir, true, 1, duplicateFileStream, start);
				start += 5;
			}
		}
	}

	public static List<Path> createFilesData(FileSystem fs, Path minDir,
			int filesCount, int start) throws Exception {
		fs.mkdirs(minDir);
		List<Path> filesList = new ArrayList<Path>();
		Path path;
		for (int j = start; j < filesCount + start; j++) {
			filesList.add(new Path("file" + j));
			path= new Path(minDir, filesList.get(j-start));
			LOG.debug("Creating Test Data with filename [" + path);
			FSDataOutputStream streamout = fs.create(path);
			streamout.writeBytes("Creating Test data for teststream"
					+ filesList.get(j-start));
			streamout.close();
			Assert.assertTrue(fs.exists(path));
		}
		return filesList;
	}

	private void testLocalMergeStreams(List<String> streamNames, List<Path>
			expectedPaths, MergeStreamDataConsistency obj) throws Exception {
		List<Path> inconsistentdata = new ArrayList<Path>();
		inconsistentdata = obj.listingValidation(mergedStreamUrl, localStreamUrls,
				streamNames);
		Assert.assertEquals(inconsistentdata.size(), expectedPaths.size());
		Assert.assertTrue(inconsistentdata.containsAll(expectedPaths));
	}

	@Test
	public void testMergeStream() throws Exception {
		MergeStreamDataConsistency obj = new MergeStreamDataConsistency();
		testLocalMergeStreams(emptyStreamName, emptyPaths, obj);
		testLocalMergeStreams(emptyDirStreamName, emptyDirsPaths, obj);
		testLocalMergeStreams(missedFilesStreamName, missedFilePaths, obj);
		testLocalMergeStreams(consistentDataStreamName, emptyPaths, obj);
		testLocalMergeStreams(dataReplayFilesStreamName, dataReplayFilePaths, obj);
		testLocalMergeStreams(extrafilesStreamName, extraFilePaths, obj);
		testLocalMergeStreams(singleFileStreamName, emptyPaths, obj);
		testLocalMergeStreams(missingSingleFileStreamName, missingSingleFilePath, obj);
		testLocalMergeStreams(inconsistencyAtEndOfStream, inconsistencyEndPaths, obj);
		testLocalMergeStreams(purgedPathStreaName, purgedPaths, obj);
    testLocalMergeStreams(purgedOnLocalStream, purgedLocalPaths, obj);
    testLocalMergeStreams(duplicateFileStream, duplicateFilePaths, obj);
		
		LOG.info("all streams together");
		List<Path> allStreamPaths = new ArrayList<Path>();
		allStreamPaths.addAll(emptyPaths);
		allStreamPaths.addAll(emptyDirsPaths);
		allStreamPaths.addAll(missedFilePaths);
		allStreamPaths.addAll(dataReplayFilePaths);
		allStreamPaths.addAll(extraFilePaths);
		allStreamPaths.addAll(missingSingleFilePath);
		allStreamPaths.addAll(inconsistencyEndPaths);
		allStreamPaths.addAll(purgedPaths);
    allStreamPaths.addAll(purgedLocalPaths);
    allStreamPaths.addAll(duplicateFilePaths);

		testLocalMergeStreams(allStreamNames, allStreamPaths, obj);
		// testing run method
		List<Path> inconsistentdata = new ArrayList<Path>();
		String[] args = { ("file:///tmp/test/" + className + "/2/,file:///tmp/test/"
				+ className +"/3/"), "file:///tmp/test/" + className + "/1/"};
		LOG.info("testing run method");
		inconsistentdata = obj.run(args);
		Assert.assertEquals(inconsistentdata.size(), allStreamPaths.size());
		Assert.assertTrue(inconsistentdata.containsAll(allStreamPaths));
	}
}
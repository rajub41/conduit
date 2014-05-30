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

import java.io.IOException;
import java.util.List;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import org.apache.hadoop.fs.Path;


public abstract class CompareDataConsistency {

  /**
   * This method compares the data consistency between source and destination streams
   * @return list of inconsistency paths
   */
  public List<Path> compareDataConsistency(TreeMap<String, Path> sourceStreamFiles, 
      TreeMap<String, Path> destStreamFiles, List<Path> inconsistency) {
    Set<Entry<String, Path>> sourceStreamFileEntries = sourceStreamFiles.
        entrySet();
    Set<Entry<String, Path>> destStreamFileEntries = destStreamFiles.
        entrySet();
    Iterator<Entry<String, Path>> sourcestreamIt = sourceStreamFileEntries.iterator();
    Iterator<Entry<String, Path>> destStreamIt = destStreamFileEntries.iterator();
    String sourceStreamKey = null;
    String destStreamKey = null;
    if (sourcestreamIt.hasNext()) {
      sourceStreamKey = sourcestreamIt.next().getKey();
    }
    if (destStreamIt.hasNext()) {
      destStreamKey = destStreamIt.next().getKey();
    }
    String firstSourceStreamKey = sourceStreamKey;
    String firstDestStreamKey = destStreamKey;
    while ((sourceStreamKey != null) && (destStreamKey != null)) {
      if (!sourceStreamKey.equals(destStreamKey)) {
        if(sourceStreamKey.compareTo(destStreamKey) < 0) {
          if (firstDestStreamKey.equals(destStreamKey)) {
            System.out.println("purged path: " + sourceStreamFiles.get(sourceStreamKey));
          } else {
            System.out.println("missing path: " + sourceStreamFiles.get(sourceStreamKey));
          }
          inconsistency.add(sourceStreamFiles.get(sourceStreamKey));
          if (sourcestreamIt.hasNext()) {
            sourceStreamKey = sourcestreamIt.next().getKey();
          } else {
            sourceStreamKey = null;
          }
        } else {
          if (firstSourceStreamKey.equals(sourceStreamKey)) {
            System.out.println("purged path: " + destStreamFiles.get(destStreamKey));
          } else {
            System.out.println("data replay: " + destStreamFiles.get(destStreamKey));
          }
          inconsistency.add(destStreamFiles.get(destStreamKey));
          if (destStreamIt.hasNext()) {	
            destStreamKey = destStreamIt.next().getKey();
          } else {
            destStreamKey = null;
          }
        }
      } else {
        if (sourcestreamIt.hasNext() && !destStreamIt.hasNext()) {
          sourceStreamKey = sourcestreamIt.next().getKey();
          destStreamKey = null;
        } else if (destStreamIt.hasNext() && !sourcestreamIt.hasNext()) {
          destStreamKey = destStreamIt.next().getKey();
          sourceStreamKey = null;
        } else if (sourcestreamIt.hasNext() && destStreamIt.hasNext()) {
          sourceStreamKey = sourcestreamIt.next().getKey();
          destStreamKey = destStreamIt.next().getKey();
        } else {
          sourceStreamKey = null;
          destStreamKey = null;
        }
      }
    }
    if ((sourceStreamFiles.size() == destStreamFiles.size()) &&
        sourceStreamKey == null && destStreamKey == null) {
      System.out.println("there are no missing files");
    } else {
      if (destStreamKey == null) {
        while (sourceStreamKey != null) {
          inconsistency.add(sourceStreamFiles.get(sourceStreamKey));
          System.out.println("Files to be sent: " +
              sourceStreamFiles.get(sourceStreamKey));
          if (sourcestreamIt.hasNext()) {
            sourceStreamKey = sourcestreamIt.next().getKey();
          } else {
            sourceStreamKey = null;
          }
        }
      } else {
        while (destStreamKey != null) {
          inconsistency.add(destStreamFiles.get(destStreamKey));
          System.out.println("extra files in stream: " +
              destStreamFiles.get(destStreamKey));
          if (destStreamIt.hasNext()) {
            destStreamKey = destStreamIt.next().getKey();
          } else {
            destStreamKey = null;
          }
        }
      }
    }
    return inconsistency;
  }
}



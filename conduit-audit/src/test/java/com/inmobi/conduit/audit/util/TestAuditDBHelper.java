package com.inmobi.conduit.audit.util;

/*
 * #%L
 * Conduit Audit
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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Set;

import junit.framework.Assert;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.inmobi.conduit.audit.Filter;
import com.inmobi.conduit.audit.GroupBy;
import com.inmobi.conduit.audit.LatencyColumns;
import com.inmobi.conduit.audit.Tuple;
import com.inmobi.messaging.ClientConfig;

public class TestAuditDBHelper extends  AuditDBUtil {

  @BeforeClass
  public void setup() {
    setupDB(false);
  }

  @AfterClass
  public void shutDown() {
    super.shutDown();
  }

  @Test(priority = 1)
  public void testUpdate() {
    ClientConfig config = ClientConfig.loadFromClasspath(AuditDBConstants.FEEDER_CONF_FILE);
    AuditDBHelper helper = new AuditDBHelper(config);
    String selectStmt = helper.getSelectStmtForUpdation();
    PreparedStatement selectStatement = null;
    ResultSet rs = null;
    try {
      selectStatement = connection.prepareStatement(selectStmt);
      rs = getResultSetOfQuery(selectStatement, tuple1);
      Assert.assertNotNull(rs);
      Assert.assertFalse(rs.next());
      Assert.assertTrue(helper.update(tupleSet1));
      rs = getResultSetOfQuery(selectStatement, tuple1);
      Assert.assertNotNull(rs);
      Assert.assertTrue(rs.next());
      Assert.assertEquals(tuple1.getSent(), rs.getLong(AuditDBConstants.SENT));
      for (LatencyColumns latencyColumns : LatencyColumns.values()) {
        Long val = tuple1.getLatencyCountMap().get(latencyColumns);
        if (val == null)
          val = 0l;
        Assert.assertEquals(val, (Long) rs.getLong(latencyColumns.toString()));
      }
      Assert.assertEquals(tuple1.getLostCount(),
          (Long) rs.getLong(LatencyColumns.C600.toString()));
      Assert.assertTrue(helper.update(tupleSet2));
      rs = getResultSetOfQuery(selectStatement, tuple1);
      Assert.assertNotNull(rs);
      Assert.assertTrue(rs.next());
      Assert.assertEquals(tuple1.getSent() + tuple2.getSent(),
          rs.getLong(AuditDBConstants.SENT));
      for (LatencyColumns latencyColumns : LatencyColumns.values()) {
        Long val1 = tuple1.getLatencyCountMap().get(latencyColumns);
        if (val1 == null)
          val1 = 0l;
        Long val2 = tuple2.getLatencyCountMap().get(latencyColumns);
        if (val2 == null)
          val2 = 0l;
        Assert.assertEquals(val1 + val2, rs.getLong(latencyColumns
            .toString()));
      }
      Assert.assertEquals(tuple1.getLostCount() + tuple2.getLostCount(),
          rs.getLong(LatencyColumns.C600.toString()));
      Assert.assertTrue(helper.update(tupleSet3));
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      try {
        if (rs != null) {
          rs.close();
        }
        if (selectStatement != null) {
          selectStatement.close();
        }
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }

  private ResultSet getResultSetOfQuery(PreparedStatement selectStatement,
                                        Tuple tuple) {
    int index = 1;
    try {
      selectStatement.setLong(index++, tuple.getTimestamp().getTime());
      selectStatement.setString(index++, tuple.getHostname());
      selectStatement.setString(index++, tuple.getTopic());
      selectStatement.setString(index++, tuple.getTier());
      selectStatement.setString(index++, tuple.getCluster());
      return selectStatement.executeQuery();
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Test(priority = 2)
  public void testRetrieve() {
    GroupBy groupBy = new GroupBy("TIER,HOSTNAME,CLUSTER");
    Filter filter = new Filter("hostname="+tuple1.getHostname());
    AuditDBHelper helper = new AuditDBHelper(
        ClientConfig.loadFromClasspath(AuditDBConstants.FEEDER_CONF_FILE));
    Set<Tuple> tupleSet = helper.retrieve(toDate, fromDate, filter, groupBy);
    Assert.assertEquals(1, tupleSet.size());
    Iterator<Tuple> tupleSetIter = tupleSet.iterator();
    Assert.assertTrue(tupleSetIter.hasNext());
    Tuple returnedTuple = tupleSetIter.next();
    Assert.assertEquals(tuple1.getHostname(), returnedTuple.getHostname());
    Assert.assertEquals(tuple1.getTier(), returnedTuple.getTier());
    Assert.assertEquals(null, returnedTuple.getTopic());
    Assert.assertEquals(tuple1.getSent() + tuple2.getSent() + tuple3.getSent(),
        returnedTuple.getSent());
    for (LatencyColumns latencyColumns : LatencyColumns.values()) {
      Long val1 = tuple1.getLatencyCountMap().get(latencyColumns);
      if (val1 == null)
        val1 = 0l;
      Long val2 = tuple2.getLatencyCountMap().get(latencyColumns);
      if (val2 == null)
        val2 = 0l;
      Long val3 = tuple3.getLatencyCountMap().get(latencyColumns);
      if (val3 == null)
        val3 = 0l;
      Long val4 = returnedTuple.getLatencyCountMap().get(latencyColumns);
      if (val4 == null)
        val4 = 0l;
      Long valx = val1 + val2 + val3;
      Assert.assertEquals(valx, val4);
    }
    Assert.assertEquals((Long) (tuple1.getLostCount() + tuple2.getLostCount() +
        tuple3.getLostCount()), returnedTuple.getLostCount());
    filter = new Filter(null);
    tupleSet = helper.retrieve(toDate, fromDate, filter, groupBy);
    Assert.assertEquals(2, tupleSet.size());
  }

  @Test(priority = 3)
   public void testTuplesOrder() {
    GroupBy groupBy = new GroupBy("CLUSTER,TIER,HOSTNAME,TOPIC");
    Filter filter = new Filter("hostname="+tuple1.getHostname());
    AuditDBHelper helper = new AuditDBHelper(
        ClientConfig.loadFromClasspath(AuditDBConstants.FEEDER_CONF_FILE));
    Set<Tuple> tupleSet = helper.retrieve(toDate, fromDate, filter, groupBy);
    Assert.assertEquals(2, tupleSet.size());
    Iterator<Tuple> tupleSetIter = tupleSet.iterator();
    Assert.assertTrue(tupleSetIter.hasNext());
    Tuple returnedTuple = tupleSetIter.next();
    Assert.assertEquals(tuple1.getTopic(), returnedTuple.getTopic());
    returnedTuple = tupleSetIter.next();
    Assert.assertEquals(tuple3.getTopic(), returnedTuple.getTopic());
  }

  @Test(priority = 4)
  public void testHostnameNull() {
    GroupBy groupBy = new GroupBy("CLUSTER,TIER,TOPIC");
    Filter filter = new Filter("cluster="+tuple1.getCluster());
    AuditDBHelper helper = new AuditDBHelper(
        ClientConfig.loadFromClasspath(AuditDBConstants.FEEDER_CONF_FILE));
    Set<Tuple> tupleSet = helper.retrieve(toDate, fromDate, filter, groupBy);
    Assert.assertEquals(2, tupleSet.size());
    Iterator<Tuple> tupleSetIter = tupleSet.iterator();
    Assert.assertTrue(tupleSetIter.hasNext());
    Tuple returnedTuple = tupleSetIter.next();
    Assert.assertEquals(null, returnedTuple.getHostname());
    returnedTuple = tupleSetIter.next();
    Assert.assertEquals(null, returnedTuple.getHostname());
  }
}

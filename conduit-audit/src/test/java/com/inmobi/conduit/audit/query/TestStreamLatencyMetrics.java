package com.inmobi.conduit.audit.query;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import junit.framework.Assert;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.inmobi.conduit.audit.LatencyColumns;
import com.inmobi.conduit.audit.Tuple;
import com.inmobi.conduit.audit.util.AuditDBConstants;
import com.inmobi.conduit.audit.util.AuditDBHelper;
import com.inmobi.conduit.audit.util.AuditDBUtil;
import com.inmobi.messaging.ClientConfig;

public class TestStreamLatencyMetrics extends AuditDBUtil {
	@BeforeClass
	  public void setup() {
	    setupDB(false);
	  }

	  @AfterClass
	  public void shutDown() {
	    super.shutDown();
	  }

	 // @Test(priority = 1)
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

}

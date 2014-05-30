package com.inmobi.conduit.audit.services;

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

import com.inmobi.conduit.audit.util.AuditDBConstants;
import com.inmobi.conduit.audit.util.AuditDBHelper;
import com.inmobi.conduit.audit.util.AuditRollupTestUtil;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.Connection;
import java.util.Calendar;
import java.util.Date;

public class TestAuditRollupService extends AuditRollupTestUtil {
  private AuditRollUpService rollUpService;

  @BeforeClass
  public void setup() {
    super.setup();
    rollUpService = new AuditRollUpService(config);
    cleanUp();
  }

  private void cleanUp() {
    try {
      FileSystem fs = FileSystem.getLocal(new Configuration());
      fs.delete(
          new Path(config.getString(AuditDBConstants.CHECKPOINT_DIR_KEY)), true);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testGetFromTime() {
    Connection connection = getConnection(config);
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(currentDate);
    calendar.set(Calendar.HOUR_OF_DAY, 1);
    calendar.set(Calendar.MINUTE, 5);
    Date fromTime = rollUpService.getRollupTime();
    Assert.assertEquals(AuditDBHelper.getFirstMilliOfDay(calendar.getTime()).longValue(),
        fromTime.getTime());

    calendar.add(Calendar.HOUR_OF_DAY, 1);
    rollUpService.mark(calendar.getTime().getTime());
    fromTime = rollUpService.getRollupTime();
    Assert.assertEquals(calendar.getTime(), fromTime);

    calendar.add(Calendar.HOUR_OF_DAY, -1);
    calendar.add(Calendar.MINUTE, 5);
    rollUpService.mark(calendar.getTime().getTime());
    fromTime = rollUpService.getRollupTime();
    Assert.assertEquals(calendar.getTime(), fromTime);

    calendar.add(Calendar.MINUTE, -5);
    fromTime = rollUpService.getTimeEnrtyDailyTable(connection, true);
    Assert.assertEquals(calendar.getTime(), fromTime);
  }

  @Test
  public void testGetTableNames() {
    Calendar calendar = Calendar.getInstance();
    calendar.set(Calendar.YEAR, 2013);
    calendar.set(Calendar.MONTH, 7);
    calendar.set(Calendar.DAY_OF_MONTH, 8);
    Date currentDate = calendar.getTime();
    String srcTable = rollUpService.createTableName(currentDate, false);
    Assert.assertEquals("audit20130808", srcTable);
    String destTable = rollUpService.createTableName(currentDate, true);
    Assert.assertEquals("hourly_audit20130808", destTable);
  }

  @AfterClass
  public void shutDown() {
    super.shutDown();
    cleanUp();
  }
}

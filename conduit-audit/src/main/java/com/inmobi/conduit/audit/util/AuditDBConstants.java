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

public interface AuditDBConstants {

  public static final String FEEDER_CONF_FILE = "audit-feeder.properties";

  public static final String JDBC_DRIVER_CLASS_NAME = "jdbc.driver.class.name";
  public static final String DB_URL = "db.url";
  public static final String DB_USERNAME = "db.username";
  public static final String DB_PASSWORD = "db.password";
  public static final String MASTER_TABLE_NAME = "audit.table.master";

  public static final String TIMESTAMP = "TIMEINTERVAL";
  public static final String SENT = "SENT";
  public static final String RECEIVED = "RECEIVED";


  public static final String GANGLIA_HOST = "feeder.ganglia.host";
  public static final String GANGLIA_PORT = "feeder.ganglia.port";
  public static final String CSV_REPORT_DIR = "feeder.csv.report.dir";
  public static final String MESSAGES_PER_BATCH = "messages.batch.num";
  public static final String CONDUIT_CONF_FILE_KEY = "feeder.conduit.conf";

  public static final String ROLLUP_HOUR_KEY = "rollup.hour";
  public static final String INTERVAL_LENGTH_KEY = "rollup.intervallength.millis";
  public static final String CHECKPOINT_DIR_KEY = "rollup.checkpoint.dir";
  public static final String CHECKPOINT_KEY = "rollup.checkpoint.key";
  public static final String TILLDAYS_KEY = "rollup.tilldays";
  public static final String NUM_DAYS_AHEAD_TABLE_CREATION =
      "num.of.days.ahead.table.creation";

  public static final String DEFAULT_CHECKPOINT_KEY = "rollupChkPt";
}

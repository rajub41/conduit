package com.inmobi.conduit.metrics;

/*
 * #%L
 * Conduit Core
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

import java.util.concurrent.TimeUnit;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.SlidingTimeWindowReservoir;

/**
 * A Gauge which is wrapped around a SlidingTimeWindowGuage. This will have a
 * aggregated value for all the values collected for last n seconds.
 * 
 */
public class SlidingTimeWindowGauge implements Gauge<Long> {

  private SlidingTimeWindowReservoir stwR;

  public SlidingTimeWindowGauge(long window, TimeUnit timeUnits) {
    stwR = new SlidingTimeWindowReservoir(window, timeUnits);
  }

  public void setValue(Long value) {
    this.stwR.update(value);
  };

  public Long getValue() {
    long sumOfValues = 0;
    for (long eachValue : stwR.getSnapshot().getValues()) {
      sumOfValues += eachValue;
    }
    return sumOfValues;
  }

}

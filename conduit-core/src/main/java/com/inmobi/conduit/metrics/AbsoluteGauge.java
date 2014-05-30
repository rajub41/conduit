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

/**
 * A Gauge which can set absolute values of a metric. 
 */
public class AbsoluteGauge {

  Number value;

  public AbsoluteGauge(Number value) {
    this.value = value;

  }

  public void setValue(Number value) {
    this.value = value;
  };

  public Number getValue() {
    return this.value;
  }
}

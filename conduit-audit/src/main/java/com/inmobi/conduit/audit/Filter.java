package com.inmobi.conduit.audit;

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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


public class Filter {

  private Map<Column, List<String>> filters;

  public Filter(String input) {
    if (input == null) {
      filters = null;
    } else {
      filters = new HashMap<Column, List<String>>();
      String inputSplit[] = input.split(",");
      for (int i = 0; i < inputSplit.length; i++) {
        String tmp = inputSplit[i];
        String keyValues[] = tmp.split("=");
        if (keyValues.length != 2) {
          continue; // skip this filter as it is malformed
        }
        Column key;
        try {
          key = Column.valueOf(keyValues[0].toUpperCase());
        } catch (Exception e) {
          continue;
        }
        // User can provide multiple options for each filtered column.
        // Values for the filtered column is separated by '|' symbol
        String[] values = stripQuotes(keyValues[1]).split("\\|");
        List<String> filterValues = new ArrayList<String>();
        for (int j = 0; j < values.length; j++) {
          filterValues.add(values[j]);
        }
        filters.put(key, filterValues);
      }
    }
  }

  private static String stripQuotes(String input) {
    if (input.startsWith("'") || input.startsWith("\"")) {
      input = input.substring(1);
    }
    if (input.endsWith("'") || input.endsWith("\"")) {
      input = input.substring(0, input.length() - 1);
    }
    return input;
  }

  public boolean apply(Map<Column, String> values) {

    if (filters != null) {
      for (Entry<Column, List<String>> filter : filters.entrySet()) {
        if (!filter.getValue().contains(values.get(filter.getKey()))) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public String toString() {
    return "Filter" + filters;
  }

  public Map<Column, List<String>> getFilters() {
    return filters;
  }
}

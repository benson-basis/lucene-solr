package org.apache.solr.update;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests some basic functionality of Solr while demonstrating good
 * Best Practices for using AbstractSolrTestCase
 */
public class AnalysisErrorHandlingTest extends SolrTestCaseJ4 {


  public String getCoreName() { return "basic"; }

  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig-basic.xml","solr/analysisconfs/analysis-err-schema.xml");
  }



  @Test
  public void testMultipleUpdatesPerAdd() {
    clearIndex();
    boolean caught = false;
    try {
      h.update("<add><doc><field name=\"id\">1</field><field name=\"text\">Alas Poor Yorik</field></doc></add>");
    } catch (SolrException se) {
      assertTrue(se.getMessage().contains("Exception writing document id 1 to the index"));
      caught = true;
    }
    if (!caught) {
      fail("Failed to even throw the exception we are stewing over.");
    }
  }


}

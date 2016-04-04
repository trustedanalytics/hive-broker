/**
 * Copyright (c) 2015 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.trustedanalytics.servicebroker.hive;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class DbNameNormalizerTest {

  @Test
  public void testNormalize_dbNameInUUIDForn_returnsUnifiedName() throws Exception {
    //given
    NameNormalizer dbNameNormalizer = DbNameNormalizer.create();
    String dbName = "7e5cc4f0-0212-11e6-ab4b-00155d3d2c21";

    //when
    String actual = dbNameNormalizer.normalize(dbName);

    //then
    Assert.assertEquals("7e5cc4f0_0212_11e6_ab4b_00155d3d2c21", actual);
  }

  @Test(expected = NullPointerException.class)
  public void testNormalize_nameIsNull_throwsNLP() throws Exception {
    //give
    NameNormalizer principalNameNormalizer = PrincipalNameNormalizer.create();

    //when
    String actual = principalNameNormalizer.normalize(null);

    //then
    //throws NLP exceptions
  }

}
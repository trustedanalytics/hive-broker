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

public class PrincipalNameNormalizerTest {

  @Test
  public void testNormalize_nameNotNull_returnsUnifiedName() throws Exception {
    //give
    NameNormalizer principalNameNormalizer = PrincipalNameNormalizer.create();
    String princName = "hive/sys@CLOUDERA";

    //when
    String actual = principalNameNormalizer.normalize(princName);

    //then
    Assert.assertEquals("hive_sys@CLOUDERA", actual);
  }

  @Test(expected = NullPointerException.class)
  public void testNormalize_nameIsNull_throwsNLP() throws Exception {
    //give
    NameNormalizer principalNameNormalizer = PrincipalNameNormalizer.create();

    //when
    principalNameNormalizer.normalize(null);

    //then
    //throws NLP exceptions
  }
}
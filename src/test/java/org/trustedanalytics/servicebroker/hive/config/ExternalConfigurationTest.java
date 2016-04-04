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
package org.trustedanalytics.servicebroker.hive.config;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;

public class ExternalConfigurationTest {

  private static final String HIVE_SUPERUSER_NAME = "hive/sys@CLOUDERA";

  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  private String tempDirPath;

  @Before
  public void setUp() {
    tempDirPath = tmp.getRoot().getAbsolutePath();
  }

  @Test
  public void getKeyTabLocationTest_hiveUserAndSerializedKeyTabSet_persistKeyTabAndreturnsKeyTabFileLocation() throws IOException {
    //given
    ExternalConfiguration conf = new ExternalConfiguration();
    conf.setHiveSuperUser(HIVE_SUPERUSER_NAME);
    conf.setHiveSuperUserKeyTab(Base64.getEncoder().encodeToString("someKeytab".getBytes()));
    conf.setKeyTabsDir(tempDirPath);

    //when
    String keyTabLocation = conf.getKeyTabLocation();

    //then
    Assert.assertEquals(tempDirPath + "/hive_sys@CLOUDERA.keytab", keyTabLocation);
    Assert.assertTrue(Files.exists(Paths.get(tempDirPath + "/hive_sys@CLOUDERA.keytab")));
  }

  @Test
  public void getKeyTabLocationTest_keyTabFileAlreadyExists_returnsKeyTabFileLocationNotOverwritesExisting() throws IOException {
    //given
    ExternalConfiguration conf = new ExternalConfiguration();
    conf.setHiveSuperUser(HIVE_SUPERUSER_NAME);
    conf.setKeyTabsDir(tempDirPath);
    tmp.newFile("hive_sys@CLOUDERA.keytab");

    //when
    String keyTabLocation = conf.getKeyTabLocation();

    //then
    Assert.assertEquals(tempDirPath + "/hive_sys@CLOUDERA.keytab", keyTabLocation);
    Assert.assertTrue(Files.size(Paths.get(tempDirPath + "/hive_sys@CLOUDERA.keytab")) == 0);
  }

  @Test(expected = IOException.class)
  public void getKeyTabLocationTest_keyTabLocationIsOccupied_throwsIOException() throws IOException {
    //given
    ExternalConfiguration conf = new ExternalConfiguration();
    conf.setHiveSuperUser(HIVE_SUPERUSER_NAME);
    conf.setKeyTabsDir(tempDirPath);
    tmp.newFolder("hive_sys@CLOUDERA.keytab");

    //when
    conf.getKeyTabLocation();

    //then
    //throws IOException
  }

}
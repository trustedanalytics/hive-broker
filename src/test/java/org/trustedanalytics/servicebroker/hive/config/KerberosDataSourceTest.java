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

import org.apache.hadoop.conf.Configuration;

import org.hamcrest.core.IsInstanceOf;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import static org.mockito.Mockito.*;
import org.mockito.runners.MockitoJUnitRunner;
import org.trustedanalytics.hadoop.kerberos.KrbLoginManager;
import org.trustedanalytics.servicebroker.hive.MockConnection;
import org.trustedanalytics.servicebroker.hive.MockedJdbcDriver;

import sun.security.krb5.KrbException;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;
import javax.sql.DataSource;

@RunWith(MockitoJUnitRunner.class)
public class KerberosDataSourceTest {

  @Mock
  private Configuration hadoopConf;

  @Mock
  private KrbLoginManager loginManager;

  @Test(expected = NullPointerException.class)
  public void BuilderBuildTest_ifAnyArgumentNull_throwsNLP() throws IOException {
    //given

    //when
    DataSource ds = KerberosDataSource.Builder.create().build();

    //then
    //throws exception
  }

  @Test(expected = UnsupportedOperationException.class)
  public void getConnectionTest_authenticationUsingPassword_throwsUnsupportedOperationException() throws SQLException {
    //given
    DataSource ds = KerberosDataSource.Builder.create().connectTo("someUrl")
        .asWho("jojo")
        .useKeyTab("/some/path/to/keytab.file")
        .with(hadoopConf)
        .with(loginManager).build();

    //when
    ds.getConnection("sdsd", "****");
    //then
    //throws exception
  }

  @Test(expected = IllegalStateException.class)
  public void getConnectionTest_loginManagerThrowsLoginException_throwsIllegalStateException()
      throws LoginException, KrbException, SQLException {
    //given
    DataSource ds = KerberosDataSource.Builder.create().connectTo("someUrl")
        .asWho("jojo")
        .useKeyTab("/some/path/to/keytab.file")
        .with(hadoopConf)
        .with(loginManager).build();
    when(loginManager.loginWithKeyTab(anyString(), anyString())).thenThrow(LoginException.class);
    //when
    ds.getConnection();
    //then
    //throws IllegalStateException
  }

  @Test(expected = IllegalStateException.class)
  public void getConnectionTest_loginManagerThrowsIO_throwsIllegalStateException()
      throws LoginException, KrbException, SQLException, IOException {
    //given
    DataSource ds = KerberosDataSource.Builder.create().connectTo("someUrl")
        .asWho("jojo")
        .useKeyTab("/some/path/to/keytab.file")
        .with(hadoopConf)
        .with(loginManager).build();
    Subject subject = new Subject();
    when(loginManager.loginWithKeyTab(anyString(), anyString())).thenReturn(subject);
    doThrow(IOException.class).when(loginManager).loginInHadoop(subject, hadoopConf);

    //when
    ds.getConnection();
    //then
    //throws IllegalStateException
  }

  @Test
  public void getConnectionTest_registerDriver_returnsConnection() throws SQLException {
    //given
    DataSource ds = KerberosDataSource.Builder.create()
        .connectTo("jdbc:hive2://localhost:10000/")
        .asWho("jojo")
        .useKeyTab("/some/path/to/keytab.file")
        .with(hadoopConf)
        .with(loginManager).build();
    ((KerberosDataSource)ds).JDBC_DRIVER = MockedJdbcDriver.class.getCanonicalName();

    //when
    Connection conn = ds.getConnection();

    //then
    Assert.assertThat(conn, IsInstanceOf.instanceOf(MockConnection.class));
  }
}
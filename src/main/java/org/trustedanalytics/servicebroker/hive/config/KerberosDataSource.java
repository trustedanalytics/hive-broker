/**
 * Copyright (c) 2015 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.trustedanalytics.servicebroker.hive.config;

import com.beust.jcommander.Strings;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.springframework.jdbc.datasource.AbstractDataSource;
import org.trustedanalytics.hadoop.config.client.AppConfiguration;
import org.trustedanalytics.hadoop.config.client.Configurations;
import org.trustedanalytics.hadoop.config.client.Property;
import org.trustedanalytics.hadoop.config.client.ServiceInstanceConfiguration;
import org.trustedanalytics.hadoop.config.client.ServiceType;
import org.trustedanalytics.hadoop.kerberos.KrbLoginManager;
import org.trustedanalytics.hadoop.kerberos.KrbLoginManagerFactory;

import sun.security.krb5.KrbException;

import java.io.IOException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;
import javax.sql.DataSource;

public final class KerberosDataSource extends AbstractDataSource implements DataSource {

  String JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";
  private static final String
      TICKET_CACHE_PATH_PARAM = "hadoop.security.kerberos.ticket.cache.path";

  private final String jdbcUrl;
  private final String user;
  private final String keyTabLocation;
  private final Configuration hadoopConf;
  private KrbLoginManager loginManager;

  private KerberosDataSource(String jdbcUrl,
                     String user,
                     String keyTabLocation,
                     Configuration hadoopConf) {

    this.jdbcUrl = jdbcUrl;
    this.user = user;
    this.keyTabLocation = keyTabLocation;
    this.hadoopConf = hadoopConf;
  }

  @Override
  public Connection getConnection(String user, String pass) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Connection getConnection() {
    try {
      Subject subject = loginManager.loginWithKeyTab(user, keyTabLocation);
      loginManager.loginInHadoop(subject, hadoopConf);
      UserGroupInformation ugi =
          UserGroupInformation.getBestUGI(hadoopConf.get(TICKET_CACHE_PATH_PARAM), user);
      return getConnection(ugi);
    } catch (LoginException
        | PrivilegedActionException
        | KrbException
        | IOException
        | InterruptedException e) {
      throw new IllegalStateException(
          String.format("Could not login %s with keytab %s", user, keyTabLocation), e);
    }
  }

  private Connection getConnection(UserGroupInformation signedOnUserSubject)
      throws PrivilegedActionException, IOException, InterruptedException {
    return (Connection) signedOnUserSubject.doAs((PrivilegedExceptionAction<Object>) () -> {
      Class.forName(JDBC_DRIVER);
      return DriverManager.getConnection(jdbcUrl, null, null);
    });
  }

  static class Builder {
    private KrbLoginManager loginManager;
    private String connectionString;
    private String keyTabLocaton;
    private Configuration hadoopConf;
    private String user;

    static Builder create() {
      return new Builder();
    }

    private Builder() {
    }

    KerberosDataSource build() {
      KerberosDataSource dataSource =
          new KerberosDataSource(Objects.requireNonNull(this.connectionString,
                                                        "Connection string not set!"),
                                 Objects.requireNonNull(this.user,
                                                        "User name not set!"),
                                 Objects.requireNonNull(this.keyTabLocaton,
                                                        "KeyTab localization not set!"),
                                 Objects.requireNonNull(this.hadoopConf,
                                                        "HadoopConf not set!"));
      dataSource.loginManager = Objects.requireNonNull(this.loginManager,
                                                       "LoginManager not set!");
      return dataSource;
    }

    Builder connectTo(String connectionString) {
      this.connectionString = connectionString;
      return this;
    }

    Builder asWho(String user) {
      this.user = user;
      return this;
    }

    Builder useKeyTab(String keyTabLocaton) {
      this.keyTabLocaton = keyTabLocaton;
      return this;
    }

    Builder with(Configuration hadoopConf) {
      this.hadoopConf = hadoopConf;
      return this;
    }

    Builder with(KrbLoginManager loginManager) {
      this.loginManager = loginManager;
      return this;
    }
  }
}
# hive-broker

Cloud foundry broker for HIVE.

# How to use it?
To use hive-broker, you need to build it from sources configure, deploy, create instance and bind it to your app. Follow steps described below.

## Build
Run command for compile and package.:
```
mvn clean package
```

## Kerberos configuration
Broker automatically bind to an existing kerberos provide service. This will provide default kerberos configuration, for REALM and KDC host. Before deploy check:

## Deploy
Push broker binary code to cloud foundry (use cf client).:
```
cf push hive-broker -p target/hive-broker-*.jar -m 512M -i 1 --no-start
```

## Configure
For strict separation of config from code (twelve-factor principle), configuration must be placed in environment variables.

Broker configuration params list (environment properties):

* obligatory :
  * USER_PASSWORD - password to interact with service broker
  * BASE_GUID - base id for catalog plan creation (uuid)
  * HADOOP_PROVIDED_ZIP - list of hive configuration parameters exposed by service (json format, default: {})
  * HIVE_SERVER2_THRIFT_BIND_HOST - hive server2 host address
  * HIVE_SERVER2_THRIFT_PORT - hive server2 port
  * SYSTEM_USER - user that has an access to broker store
  * SYSTEM_USER_PASSWORD - password for SYSTEM_USER
* optional :
  * HIVE_SUPERUSER - superuser principal name (user with full permissions on hive server) 
  * HIVE_SUPERUSER_KEYTAB - base64 encoded keytab for HIVE_SUPERUSER
  * CF_CATALOG_SERVICENAME - service name in cloud foundry catalog (default: hive)
  * CF_CATALOG_SERVICEID - service id in cloud foundry catalog (default: hive)

## Injection of Hive client configuration
HIVE configuration must be set via HADOOP_PROVIDED_ZIP environment variable. Description of this process is this same as in HDFS broker case ( https://github.com/trustedanalytics/hdfs-broker/ ).

## Zookeeper configuration
Broker instance should be bind with zookeeper broker instance to get zookeeper configuration.
```
cf bs <app> <zookeeper-instance>
```
and kerberos service instance, only for kerberized environments.
```
cf bs <app> <kerberos-instance>
```

## Start service broker application

Use cf client :
```
cf start hive-broker
```
## Create new service instance

Use cf client :
```
cf create-service-broker hive-broker <user> <password> https://hive-broker.<platform_domain>
cf enable-service-access hive
cf cs hive shared hive-instance
```

## Binding broker instance

Broker instance can be bind with cf client :
```
cf bs <app> hive-instance
```
or by configuration in app's manifest.yml :
```yaml
  services:
    - hive-instance
```

To check if broker instance is bound, use cf client :
```
cf env <app>
```
and look for :
```yaml
  "hive": [
   {
    "credentials": {
     "HADOOP_CONFIG_KEY": {
      <hive_configuration_json>
     },
     "HADOOP_CONFIG_ZIP": {
      "description": "This is the encoded zip file of hadoop-configuration",
      "encoded_zip": "<base64 of hive client configuration>"
     },
     "connectionUrl": "JDBC connection string ready to use with Jdbc Driver: 
                       jdbc:hive2://<hive_server2_host>:<hive_server2_port>/<database_name>"
    },
    "label": "hive",
    "name": "hive-instance",
    "plan": "shared",
    "tags": []
   }
  ]
```
in VCAP_SERVICES.

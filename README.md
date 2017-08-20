# ignite-hbase

[![Build Status](https://travis-ci.org/bakdata/ignite-hbase.svg?branch=master)](https://travis-ci.org/bakdata/ignite-hbase)
[![Quality Gate](https://sonarcloud.io/api/badges/gate?key=com.bakdata:ignite-hbase:master)](https://sonarcloud.io/dashboard/index/com.bakdata:ignite-hbase:master)

This [Apache Ignite](https://ignite.apache.org/) plugin adds support for using [Apache HBase](https://hbase.apache.org/) as [persistent store](https://apacheignite.readme.io/docs/persistent-store).
It allows for using Ignite Data Grid as a high-perfromance key-value store with the benefit of persisting your data in HBase without sacrificing any performance. 

Adding ignite-hbase to your Ignite is straight forward.

Download or build the jar and add it to your `USER_LIBS`:

~~~shell
export USER_LIBS=$USER_LIBS:/path/to/ignite-hbase-0.0.1-SNAPSHOT-jar-with-dependencies.jar
~~~

You can then enable it in your Ignite XML configuration

~~~xml
<property name="cacheConfiguration">
    <!-- enable persistence for your cache -->
    <property name="readThrough" value="true"/>
    <property name="writeThrough" value="true"/>
    
    <!-- configure hbase as persistent store -->
    <property name="cacheStoreFactory">
        <bean class="javax.cache.configuration.FactoryBuilder$SingletonFactory">
            <constructor-arg>
                <bean class="com.bakdata.ignite.hbase.HBaseCacheStore"/>
            </constructor-arg>
        </bean>
    </property>
    <property name="cacheStoreSessionListenerFactories">
        <list>
            <bean class="javax.cache.configuration.FactoryBuilder$SingletonFactory">
                <constructor-arg>
                    <bean class="com.bakdata.ignite.hbase.HBaseCacheStoreSessionListener">
                        <property name="tableName" value="MYTABLE"/> <!--required-->
                        <property name="properties">
                            <props>
                                <prop key="hbase.master">192.168.50.4:60000</prop>
                            </props>
                        </property>
                    </bean>
                </constructor-arg>
            </bean>
        </list>
    </property>
</property>
~~~

or in your Java code

~~~java
CacheConfiguration<String, Long> config = new CacheConfiguration<>();

// enable persistence for your cache
config.setReadThrough(true);
config.setWriteThrough(true);

// configure hbase as persistent store
HBaseCacheStore<String, Long> cs = new HBaseCacheStore<>();
config.setCacheStoreFactory(FactoryBuilder.factoryOf(cs));
HBaseCacheStoreSessionListener cssl = new HBaseCacheStoreSessionListener("MYTABLE");
cssl.addProperty("hbase.master", "192.168.50.4:60000");
config.setCacheStoreSessionListenerFactories(FactoryBuilder.factoryOf(cssl));
~~~

You do not have to bother with creating HBase tables since ignite-hbase will take care for you.

## Configuration

You can configure ignite-hbase to your needs.

First of all, we recommend to enable write-behind for your cache.
This built-in Ignite feature asynchronously writes values to persistent store rather than immediatly persisting your data.
Enable it like this:

~~~xml
<property name="cacheConfiguration">
    <property name="writeBehindEnabled" value="true"/>
</property>
~~~

~~~java
config.setWriteBehindEnabled(true);
~~~

Since HBase only stores byte arrays, your objects need to be serialized.
By default, Java Object Serialization is used.
This is far from being optimal since the footprint of the serialized objects is usually larger than needed.
Therefore, you can specify how your objects should be serialized like this:

~~~xml
<property name="cacheStoreFactory">
    <bean class="javax.cache.configuration.FactoryBuilder$SingletonFactory">
        <constructor-arg>
            <bean class="com.bakdata.ignite.hbase.HBaseCacheStore">
                <property name="keySerializer">
                    <bean class="com.bakdata.commons.serialization.StringSerializer"/>
                </property>
                <property name="valueSerializer">
                    <bean class="com.bakdata.commons.serialization.LongSerializer"/>
                </property>
            </bean>
        </constructor-arg>
    </bean>
</property>
~~~

~~~java
cs.setKeySerializer(new StringSerializer());
cs.setValueSerializer(new LongSerializer());
~~~

We provide implementations for all Java primitives but feel free to add your own serialization by implementing the `Serializer` interface.

Finally, there are several ways to configure the connection to HBase.
You can set connection properties as seen above:

~~~java
cssl.addProperty("hbase.master", "192.168.50.4:60000");
~~~

Or like this:

~~~xml
<property name="cacheStoreSessionListenerFactories">
    <list>
        <bean class="javax.cache.configuration.FactoryBuilder$SingletonFactory">
            <constructor-arg>
                <bean class="com.bakdata.ignite.hbase.HBaseCacheStoreSessionListener">
                    <property name="properties">
                        <props>
                            <prop key="hbase.master">192.168.50.4:60000</prop>
                        </props>
                    </property>
                </bean>
            </constructor-arg>
        </bean>
    </list>
</property>
~~~

~~~java
Properties properties = new Properties();
properties.setProperty("hbase.master", "192.168.50.4:60000");
cssl.setProperties(properties);
~~~

Or you can specify a path to an HBase configuration file:

~~~xml
<property name="cacheStoreSessionListenerFactories">
    <list>
        <bean class="javax.cache.configuration.FactoryBuilder$SingletonFactory">
            <constructor-arg>
                <bean class="com.bakdata.ignite.hbase.HBaseCacheStoreSessionListener">
                    <property name="confPath" value="path/to/hbase-site.xml"/>
                </bean>
            </constructor-arg>
        </bean>
    </list>
</property>
~~~

~~~java
cssl.setConfPath("path/to/hbase-site.xml");
~~~

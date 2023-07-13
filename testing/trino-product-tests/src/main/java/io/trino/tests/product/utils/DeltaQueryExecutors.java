/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.tests.product.utils;

import com.google.common.collect.ImmutableList;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airlift.log.Logger;
import io.airlift.resolver.ArtifactResolver;
import io.airlift.resolver.DefaultArtifact;
import io.trino.tempto.context.TestContext;
import io.trino.tempto.query.JdbcConnectionsPool;
import io.trino.tempto.query.JdbcConnectivityParamsState;
import io.trino.tempto.query.JdbcQueryExecutor;
import io.trino.tempto.query.QueryExecutor;
import org.sonatype.aether.artifact.Artifact;

import javax.annotation.concurrent.GuardedBy;

import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.resolver.ArtifactResolver.MAVEN_CENTRAL_URI;
import static io.airlift.resolver.ArtifactResolver.USER_LOCAL_REPO;
import static io.trino.tempto.context.ThreadLocalTestContextHolder.testContext;
import static java.lang.ClassLoader.getPlatformClassLoader;

public final class DeltaQueryExecutors
{
    private static final Logger log = Logger.get(DeltaQueryExecutors.class);

    @GuardedBy("DRIVERS")
    private static final Map<String, Driver> DRIVERS = new HashMap<>();

    private static final RetryPolicy<Driver> loadDatabaseDriverRetryPolicy = RetryPolicy.<Driver>builder()
            .withMaxRetries(30)
            .withDelay(Duration.ofSeconds(10))
            .onRetry(event -> log.warn(event.getLastException(), "Download failed on attempt %d, will retry.", event.getAttemptCount()))
            .build();

    private static final DeltaConnectionsPool deltaConnectionPool = new DeltaConnectionsPool();

    private DeltaQueryExecutors() {}

    public static JdbcQueryExecutor configureConnectionsPool(TestContext testContext)
    {
        JdbcQueryExecutor jdbcQueryExecutor = (JdbcQueryExecutor) testContext().getDependency(QueryExecutor.class, "delta");
        try {
            Field field = jdbcQueryExecutor.getClass().getDeclaredField("jdbcConnectionsPool");
            field.setAccessible(true);
            field.set(jdbcQueryExecutor, deltaConnectionPool);
            testContext.registerCloseCallback(context -> deltaConnectionPool.close());
            return jdbcQueryExecutor;
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private static class DeltaConnectionsPool
            extends JdbcConnectionsPool
    {
        private final Map<JdbcConnectivityParamsState, Connection> connections = new HashMap<>();

        @Override
        public Connection connectionFor(JdbcConnectivityParamsState jdbcParamsState)
        {
            Driver driver;
            synchronized (DRIVERS) {
                driver = DRIVERS.computeIfAbsent(jdbcParamsState.driverClass, className -> Failsafe.with(loadDatabaseDriverRetryPolicy)
                        .get(() -> loadDatabaseDriver(className)));
            }

            Properties properties = new Properties();
            properties.put("user", jdbcParamsState.user);
            properties.put("password", jdbcParamsState.password);

            return connections.computeIfAbsent(jdbcParamsState, params -> {
                try {
                    return driver.connect(jdbcParamsState.url, properties);
                }
                catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        public void close()
        {
            connections.clear();
        }
    }

    private static Driver loadDatabaseDriver(String driverClassName)
    {
        // TODO Add support for maven coordinate in tempto
        ArtifactResolver resolver = new ArtifactResolver(USER_LOCAL_REPO, ImmutableList.of(MAVEN_CENTRAL_URI));
        List<URL> classPath = resolver.resolveArtifacts(getArtifact(driverClassName)).stream()
                .map(artifact -> {
                    try {
                        return artifact.getFile().toURI().toURL();
                    }
                    catch (MalformedURLException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(toImmutableList());
        checkArgument(!classPath.isEmpty(), "classPath must not be empty");

        @SuppressWarnings("resource")
        URLClassLoader classLoader = new URLClassLoader(classPath.toArray(URL[]::new), getPlatformClassLoader());
        Class<? extends Driver> driverClass;
        try {
            driverClass = classLoader.loadClass(driverClassName).asSubclass(Driver.class);
        }
        catch (ClassNotFoundException | ClassCastException e) {
            throw new RuntimeException("Failed to load Driver class: " + driverClassName, e);
        }

        try {
            return driverClass.getConstructor().newInstance();
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException("Failed to create instance of Driver: " + driverClassName, e);
        }
    }

    private static Artifact getArtifact(String driverClass)
    {
        if (driverClass.equals("org.apache.hive.jdbc.HiveDriver")) {
            // Resolve maven artifact at runtime to use different Hive JDBC drivers between Hive and OSS Delta Lake tests because
            // the old Hive JDBC driver (0.13.1) can't connect to new Delta Lake (2.4.0) and
            // the new Hive JDBC driver (>= 3.0.0) supports the above Delta Lake version, but it can't connect to old Hive servers (1.2.1).
            return new DefaultArtifact("org.apache.hive:hive-jdbc:jar:standalone:3.1.3");
        }
        if (driverClass.equals("com.databricks.client.jdbc.Driver")) {
            return new DefaultArtifact("com.databricks:databricks-jdbc:2.6.32");
        }
        throw new IllegalArgumentException("Unexpected driver class: " + driverClass);
    }
}

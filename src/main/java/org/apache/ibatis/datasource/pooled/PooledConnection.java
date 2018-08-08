/**
 *    Copyright ${license.git.copyrightYears} the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.apache.ibatis.datasource.pooled;

import org.apache.ibatis.reflection.ExceptionUtil;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * 使用动态代理的方式封装了实际的连接
 * @author Clinton Begin
 */
class PooledConnection implements InvocationHandler {

  private static final String CLOSE = "close";
  private static final Class<?>[] IFACES = new Class<?>[] { Connection.class };

  private final int hashCode;
  /**
   * 该连接所对应的数据源，代表创建这个连接的数据源，同时关闭的时候也归还到这个数据源中
   */
  private final PooledDataSource dataSource;
  /**
   * 真正的数据库连接
   */
  private final Connection realConnection;
  /**
   * 连接代理对象
   */
  private final Connection proxyConnection;
  /**
   * 从数据源取出连接的时间
   */
  private long checkoutTimestamp;
  /**
   * 连接创建的时间
   */
  private long createdTimestamp;
  /**
   * 连接最后一次使用的时间
   */
  private long lastUsedTimestamp;
  /**
   * 根据数据库url、用户名、密码生成一个hash值，唯一标识一个连接池
   */
  private int connectionTypeCode;
  /**
   * 连接是否有效
   */
  private boolean valid;

  /*
   * Constructor for SimplePooledConnection that uses the Connection and PooledDataSource passed in
   *
   * @param connection - the connection that is to be presented as a pooled connection
   * @param dataSource - the dataSource that the connection is from
   */
  public PooledConnection(Connection connection, PooledDataSource dataSource) {
    this.hashCode = connection.hashCode();
    this.realConnection = connection;
    this.dataSource = dataSource;
    this.createdTimestamp = System.currentTimeMillis();
    this.lastUsedTimestamp = System.currentTimeMillis();
    this.valid = true;
    //动态代理生成连接代理对象
    this.proxyConnection = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), IFACES, this);
  }

  /*
   * Invalidates the connection
   */
  public void invalidate() {
    valid = false;
  }

  /*
   * Method to see if the connection is usable
   *
   * @return True if the connection is usable
   */
  public boolean isValid() {
    return valid && realConnection != null && dataSource.pingConnection(this);
  }

  /*
   * Getter for the *real* connection that this wraps
   *
   * @return The connection
   */
  public Connection getRealConnection() {
    return realConnection;
  }

  /*
   * Getter for the proxy for the connection
   *
   * @return The proxy
   */
  public Connection getProxyConnection() {
    return proxyConnection;
  }

  /*
   * Gets the hashcode of the real connection (or 0 if it is null)
   *
   * @return The hashcode of the real connection (or 0 if it is null)
   */
  public int getRealHashCode() {
    return realConnection == null ? 0 : realConnection.hashCode();
  }

  /*
   * Getter for the connection type (based on url + user + password)
   *
   * @return The connection type
   */
  public int getConnectionTypeCode() {
    return connectionTypeCode;
  }

  /*
   * Setter for the connection type
   *
   * @param connectionTypeCode - the connection type
   */
  public void setConnectionTypeCode(int connectionTypeCode) {
    this.connectionTypeCode = connectionTypeCode;
  }

  /*
   * Getter for the time that the connection was created
   *
   * @return The creation timestamp
   */
  public long getCreatedTimestamp() {
    return createdTimestamp;
  }

  /*
   * Setter for the time that the connection was created
   *
   * @param createdTimestamp - the timestamp
   */
  public void setCreatedTimestamp(long createdTimestamp) {
    this.createdTimestamp = createdTimestamp;
  }

  /*
   * Getter for the time that the connection was last used
   *
   * @return - the timestamp
   */
  public long getLastUsedTimestamp() {
    return lastUsedTimestamp;
  }

  /*
   * Setter for the time that the connection was last used
   *
   * @param lastUsedTimestamp - the timestamp
   */
  public void setLastUsedTimestamp(long lastUsedTimestamp) {
    this.lastUsedTimestamp = lastUsedTimestamp;
  }

  /*
   * Getter for the time since this connection was last used
   *
   * @return - the time since the last use
   */
  public long getTimeElapsedSinceLastUse() {
    return System.currentTimeMillis() - lastUsedTimestamp;
  }

  /*
   * Getter for the age of the connection
   *
   * @return the age
   */
  public long getAge() {
    return System.currentTimeMillis() - createdTimestamp;
  }

  /*
   * Getter for the timestamp that this connection was checked out
   *
   * @return the timestamp
   */
  public long getCheckoutTimestamp() {
    return checkoutTimestamp;
  }

  /*
   * Setter for the timestamp that this connection was checked out
   *
   * @param timestamp the timestamp
   */
  public void setCheckoutTimestamp(long timestamp) {
    this.checkoutTimestamp = timestamp;
  }

  /*
   * Getter for the time that this connection has been checked out
   *
   * @return the time
   */
  public long getCheckoutTime() {
    return System.currentTimeMillis() - checkoutTimestamp;
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  /*
   * Allows comparing this connection to another
   *
   * @param obj - the other connection to test for equality
   * @see Object#equals(Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof PooledConnection) {
      return realConnection.hashCode() == ((PooledConnection) obj).realConnection.hashCode();
    } else if (obj instanceof Connection) {
      return hashCode == obj.hashCode();
    } else {
      return false;
    }
  }

  /**
   * 数据库连接增强，使用前校验连接是否有效，关闭后对连接进行回收
   * Required for InvocationHandler implementation.
   *
   * @param proxy  - not used
   * @param method - the method to be executed
   * @param args   - the parameters to be passed to the method
   * @see java.lang.reflect.InvocationHandler#invoke(Object, java.lang.reflect.Method, Object[])
   */
  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    String methodName = method.getName();
    //调用close方法的时候并非真正关闭数库连接，而是归还到连接池中
    if (CLOSE.hashCode() == methodName.hashCode() && CLOSE.equals(methodName)) {
      //如果调用连接的close方法。则调用方法归还连接
      dataSource.pushConnection(this);
      return null;
    } else {
      try {
        if (!Object.class.equals(method.getDeclaringClass())) {
          // issue #579 toString() should never fail
          // throw an SQLException instead of a Runtime
          //检查连接的有效性
          checkConnection();
        }
        return method.invoke(realConnection, args);
      } catch (Throwable t) {
        throw ExceptionUtil.unwrapThrowable(t);
      }
    }
  }

  private void checkConnection() throws SQLException {
    if (!valid) {
      throw new SQLException("Error accessing PooledConnection. Connection is invalid.");
    }
  }

}

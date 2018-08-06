/**
 *    Copyright 2009-2018 the original author or authors.
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
package org.apache.ibatis.cache;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.ibatis.reflection.ArrayUtil;

/**
 * Mybatis中涉及到动态SQL的原因，缓存项的key不能仅仅通过一个String来表示，
 * 所以通过CacheKey来封装缓存的Key值，CacheKey可以封装多个影响缓存项的因素；
 * 判断两个CacheKey是否相同关键是比较两个对象的hash值是否一致；
 *
 * 构成CacheKey的对象
 * mappedStatment的id
 * 指定查询结果集的范围（分页信息）
 * 查询所使用的SQL语句
 * 用户传递给SQL语句的实际参数值
 * @author Clinton Begin
 */
public class CacheKey implements Cloneable, Serializable {

  private static final long serialVersionUID = 1146682552656046210L;

  public static final CacheKey NULL_CACHE_KEY = new NullCacheKey();

  private static final int DEFAULT_MULTIPLYER = 37;
  private static final int DEFAULT_HASHCODE = 17;

  /**
   * 参与hash计算的乘数
   */
  private final int multiplier;
  /**
   * CacheKey的hash值，在update函数中实时运算出来的
   */
  private int hashcode;
  /**
   * 校验和，hash值的和
   */
  private long checksum;
  /**
   * updateList中的元素个数
   */
  private int count;
  // 8/21/2017 - Sonarlint flags this as needing to be marked transient.  While true if content is not serializable, this is not always true and thus should not be marked transient.
  /**
   * 该集合中的元素比较确定CacheKey是否相等
   */
  private List<Object> updateList;

  public CacheKey() {
    this.hashcode = DEFAULT_HASHCODE;
    this.multiplier = DEFAULT_MULTIPLYER;
    this.count = 0;
    this.updateList = new ArrayList<>();
  }

  public CacheKey(Object[] objects) {
    this();
    updateAll(objects);
  }

  public int getUpdateCount() {
    return updateList.size();
  }

  public void update(Object object) {
    //获取object的hash值
    int baseHashCode = object == null ? 1 : ArrayUtil.hashCode(object);
    //更新count、checksum以及hashcode的值
    count++;
    checksum += baseHashCode;
    baseHashCode *= count;

    hashcode = multiplier * hashcode + baseHashCode;
    //将对象添加到updateList中
    updateList.add(object);
  }

  public void updateAll(Object[] objects) {
    for (Object o : objects) {
      update(o);
    }
  }

  @Override
  public boolean equals(Object object) {
    //比较是不是同一个对象
    if (this == object) {
      return true;
    }
    //比较是否相同类型
    if (!(object instanceof CacheKey)) {
      return false;
    }

    final CacheKey cacheKey = (CacheKey) object;
    //hashcode是否相同
    if (hashcode != cacheKey.hashcode) {
      return false;
    }
    //checksum是否相同
    if (checksum != cacheKey.checksum) {
      return false;
    }
    //count是否相同
    if (count != cacheKey.count) {
      return false;
    }
    //如果以上都相同，则按顺序比较updateList中元素的hash值是否一致
    for (int i = 0; i < updateList.size(); i++) {
      Object thisObject = updateList.get(i);
      Object thatObject = cacheKey.updateList.get(i);
      if (!ArrayUtil.equals(thisObject, thatObject)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    return hashcode;
  }

  @Override
  public String toString() {
    StringBuilder returnValue = new StringBuilder().append(hashcode).append(':').append(checksum);
    for (Object object : updateList) {
      returnValue.append(':').append(ArrayUtil.toString(object));
    }
    return returnValue.toString();
  }

  @Override
  public CacheKey clone() throws CloneNotSupportedException {
    CacheKey clonedCacheKey = (CacheKey) super.clone();
    clonedCacheKey.updateList = new ArrayList<>(updateList);
    return clonedCacheKey;
  }

}

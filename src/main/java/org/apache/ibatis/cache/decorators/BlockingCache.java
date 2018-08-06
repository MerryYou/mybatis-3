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
package org.apache.ibatis.cache.decorators;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.ibatis.cache.Cache;
import org.apache.ibatis.cache.CacheException;

/**
 * Simple blocking decorator 
 * 
 * Simple and inefficient version of EhCache's BlockingCache decorator.
 * It sets a lock over a cache key when the element is not found in cache.
 * This way, other threads will wait until this element is filled instead of hitting the database.
 * 
 * @author Eduardo Macarron
 * 阻塞缓存装饰器，保证同一时刻只有一个线程查询数据
 */
public class BlockingCache implements Cache {
  /**
   * 阻塞超时时间
   */
  private long timeout;
  /**
   * 被装饰的缓存对象，一般是PerpetualCache
   */
  private final Cache delegate;
  /**
   * 锁对象，粒度到key值
   */
  private final ConcurrentHashMap<Object, ReentrantLock> locks;

  public BlockingCache(Cache delegate) {
    this.delegate = delegate;
    this.locks = new ConcurrentHashMap<>();
  }

  @Override
  public String getId() {
    return delegate.getId();
  }

  @Override
  public int getSize() {
    return delegate.getSize();
  }

  @Override
  public void putObject(Object key, Object value) {
    try {
      delegate.putObject(key, value);
    } finally {
      releaseLock(key);
    }
  }

  @Override
  public Object getObject(Object key) {
    //根据key获得锁对象，获取锁成功加锁，获取锁失败阻塞一段时间重试
    acquireLock(key);
    Object value = delegate.getObject(key);
    if (value != null) {
      //获取数据成功时释放锁
      releaseLock(key);
    }        
    return value;
  }

  @Override
  public Object removeObject(Object key) {
    // despite of its name, this method is called only to release locks
    releaseLock(key);
    return null;
  }

  @Override
  public void clear() {
    delegate.clear();
  }

  @Override
  public ReadWriteLock getReadWriteLock() {
    return null;
  }

  /**
   * 根据key获取对象锁
   * @param key
   * @return
   */
  private ReentrantLock getLockForKey(Object key) {
    //创建锁
    ReentrantLock lock = new ReentrantLock();
    //把新锁添加到locks集合中，如果添加成功使用新锁，如果添加失败则使用locks集合中的锁
    ReentrantLock previous = locks.putIfAbsent(key, lock);
    return previous == null ? lock : previous;
  }

  /**
   * 根据key获得锁对象，获取锁成功加锁，获取锁失败阻塞一段时间重试
   * @param key
   */
  private void acquireLock(Object key) {
    //获取对象锁
    Lock lock = getLockForKey(key);
    if (timeout > 0) {
      try {
        //在一段时间内尝试获取锁
        boolean acquired = lock.tryLock(timeout, TimeUnit.MILLISECONDS);
        //超时抛出异常
        if (!acquired) {
          throw new CacheException("Couldn't get a lock in " + timeout + " for the key " +  key + " at the cache " + delegate.getId());  
        }
      } catch (InterruptedException e) {
        throw new CacheException("Got interrupted while trying to acquire lock for key " + key, e);
      }
    } else {
      //如果没有设置阻塞时间，则直接获取锁
      lock.lock();
    }
  }

  /**
   * 释放锁
   * @param key
   */
  private void releaseLock(Object key) {
    ReentrantLock lock = locks.get(key);
    if (lock.isHeldByCurrentThread()) {
      //如果锁被当前线程持有则释放锁
      lock.unlock();
    }
  }

  public long getTimeout() {
    return timeout;
  }

  public void setTimeout(long timeout) {
    this.timeout = timeout;
  }  
}
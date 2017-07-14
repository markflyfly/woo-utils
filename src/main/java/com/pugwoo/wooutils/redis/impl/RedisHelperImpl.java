package com.pugwoo.wooutils.redis.impl;

import com.pugwoo.wooutils.redis.IRedisObjectConverter;
import com.pugwoo.wooutils.redis.RedisHelper;
import com.pugwoo.wooutils.redis.RedisLimitParam;
import com.pugwoo.wooutils.redis.anatation.SOALockService;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.Signature;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * 大部分实现时间: 2016年11月2日 15:10:21
 * @author nick
 */
public class RedisHelperImpl implements RedisHelper {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(RedisHelperImpl.class);
	
	private String host = "127.0.0.1";
	private Integer port = 6379;
	private String password = null;
	/**指定0~15哪个redis库*/
	private Integer database = 0;
	/**String和Object之间的转换对象*/
	private IRedisObjectConverter redisObjectConverter;
	
	/**
	 * 单例的JedisPool，实际项目中可以配置在string，也可以是懒加载
	 */
	private static JedisPool pool = null;
	
	@Override
	public Jedis getJedisConnection() {
		if(pool == null) {
			synchronized (this) {
				if(pool == null) {
					JedisPoolConfig poolConfig = new JedisPoolConfig();
					poolConfig.setMaxTotal(3000); // 最大链接数
					poolConfig.setMaxIdle(10);
					poolConfig.setMaxWaitMillis(1000L);
					poolConfig.setTestOnBorrow(false);
					poolConfig.setTestOnReturn(false);
					if(password != null && password.trim().isEmpty()) {
						password = null;
					}
					
					pool = new JedisPool(poolConfig, host, Integer.valueOf(port),
							Protocol.DEFAULT_TIMEOUT, password, database);
				}
			}
		}
		return pool.getResource();
	}
	
	@Override
	protected void finalize() throws Throwable {
		if(pool != null && !pool.isClosed()) {
			pool.close();
		}
		super.finalize();
	}
	
	@Override
	public boolean setString(String key, int expireSecond, String value) {
		Jedis jedis = null;
		try {
			jedis = getJedisConnection();
			jedis.setex(key, expireSecond, value);
			return true;
		} catch (Exception e) {
			LOGGER.error("operate jedis error, key:{}, value:{}", key, value, e);
			return false;
		} finally {
			if (jedis != null) {
				try {
					jedis.close();
				} catch (Exception e) {
					LOGGER.error("close jedis error, key:{}, value:{}", key, value, e);
				}
			}
		}
	}
	
	@Override
	public <T> boolean setObject(String key, int expireSecond, T value) {
		if(redisObjectConverter == null) {
			throw new RuntimeException("IRedisObjectConverter is null");
		}
		String v = redisObjectConverter.convertToString(value);
		return setString(key, expireSecond, v);
	}
	
	@Override
	public boolean setStringIfNotExist(String key, int expireSecond, String value) {
		Jedis jedis = null;
		try {
			jedis = getJedisConnection();
			String result = jedis.set(key, value, "NX", "EX", expireSecond);
			return result != null;
		} catch (Exception e) {
			LOGGER.error("operate jedis error, key:{}, value:{}", key, value, e);
			return false;
		} finally {
			if (jedis != null) {
				try {
					jedis.close();
				} catch (Exception e) {
					LOGGER.error("close jedis error, key:{}, value:{}", key, value, e);
				}
			}
		}
	}
	
	@Override
	public boolean setExpire(String key, int expireSecond) {
		Jedis jedis = null;
		try {
			jedis = getJedisConnection();
			jedis.expire(key, expireSecond);
			return true;
		} catch (Exception e) {
			LOGGER.error("operate jedis error, key:{}", key, e);
			return false;
		} finally {
			if (jedis != null) {
				try {
					jedis.close();
				} catch (Exception e) {
					LOGGER.error("close jedis error, key:{}", key, e);
				}
			}
		}
	}
	
	@Override
	public long getExpireSecond(String key) {
		Jedis jedis = null;
		try {
			jedis = getJedisConnection();
			return jedis.ttl(key);
		} catch (Exception e) {
			LOGGER.error("operate jedis error, key:{}", key, e);
			return -999;
		} finally {
			if (jedis != null) {
				try {
					jedis.close();
				} catch (Exception e) {
					LOGGER.error("close jedis error, key:{}", key, e);
				}
			}
		}
	}
	
	@Override
	public String getString(String key) {
		Jedis jedis = null;
		try {
			jedis = getJedisConnection();
			String str = jedis.get(key);
			return str;
		} catch (Exception e) {
			LOGGER.error("operate jedis error, key:{}", key, e);
			return null;
		} finally {
			if (jedis != null) {
				try {
					jedis.close();
				} catch (Exception e) {
					LOGGER.error("close jedis error, key:{}", key, e);
				}
			}
		}
	}
	
	@Override
	public <T> T getObject(String key, Class<T> clazz) {
		if(redisObjectConverter == null) {
			throw new RuntimeException("IRedisObjectConverter is null");
		}
		String value = getString(key);
		if(value == null) {
			return null;
		}
		
		return redisObjectConverter.convertToObject(value, clazz);
	}
	
	@Override
	public boolean remove(String key) {
		Jedis jedis = null;
		try {
			jedis = getJedisConnection();
			jedis.del(key);
			return true;
		} catch (Exception e) {
			LOGGER.error("operate jedis error, key:{}", key, e);
			return false;
		} finally {
			if (jedis != null) {
				try {
					jedis.close();
				} catch (Exception e) {
					LOGGER.error("close jedis error, key:{}", key, e);
				}
			}
		}
	}
	
	@Override
	public boolean compareAndSet(String key, String value, String oldValue, Integer expireSeconds) {
		Jedis jedis = null;
		try {
			jedis = getJedisConnection();
			jedis.watch(key);
			String readOldValue = jedis.get(key);
			if((readOldValue == oldValue) || (readOldValue != null && readOldValue.equals(oldValue))) {
				Transaction tx = jedis.multi();
				Response<String> result = null;
				if(expireSeconds != null && expireSeconds >=0) {
					result = tx.setex(key, expireSeconds, value);
				} else {
					result = tx.set(key, value);
				}

				List<Object> results = tx.exec();
				if(results == null || result == null || result.get() == null) {
					return false;
				} else {
					return true;
				}
			} else {
				return false;
			}
		} catch (Exception e) {
			LOGGER.error("compareAndSet error,key:{}, value:{}, oldValue:{}", key, value, oldValue);
			return false;
		} finally {
			if (jedis != null) {
				try {
					jedis.close();
				} catch (Exception e) {
					LOGGER.error("close jedis error, key:{}", key, e);
				}
			}
		}
	}

	@Override
	public long getLimitCount(RedisLimitParam limitParam, String key) {
		return RedisLimit.getLimitCount(this, limitParam, key);
	}

	@Override
	public boolean hasLimitCount(RedisLimitParam limitParam, String key) {
		return RedisLimit.hasLimitCount(this, limitParam, key);
	}

	@Override
	public long useLimitCount(RedisLimitParam limitEnum, String key) {
		return RedisLimit.useLimitCount(this, limitEnum, key);
	}

	@Override
	public long useLimitCount(RedisLimitParam limitParam, String key, int count) {
		return RedisLimit.useLimitCount(this, limitParam, key, count);
	}
	
	@Override
	public boolean requireLock(String namespace, String key, int maxTransactionSeconds) {
		return RedisTransaction.requireLock(this, namespace, key, maxTransactionSeconds);
	}
	
	@Override
	public boolean releaseLock(String namespace, String key) {
		return RedisTransaction.releaseLock(this, namespace, key);
	}

	@Override
	public Long nextId(String nameSpace) {
		Jedis jedis = null;
		try{
			jedis = getJedisConnection();
			return jedis.incr(nameSpace+"_ID");
		}catch (Exception e){
			LOGGER.error("");
			return null;
		}finally {
			if (jedis != null) {
				try {
					jedis.close();
				} catch (Exception e) {
					LOGGER.error("close jedis error ", e);
				}
			}
		}


	}

	/**
	 * aop 分布式注解式事务
	 * 定义切面时指定注解所在
	 * @param point
	 * @return
	 */
	public Object distributeLock(final ProceedingJoinPoint point){
		final String name = Thread.currentThread().getName();
		// 方法处理结果
		Object resultObject;
		// 确认注解是在方法上
		Signature signature = point.getSignature();
		if(!(signature instanceof MethodSignature)){
			LOGGER.error("SOALockService not anatation in method");
			try {
				return point.proceed();
			} catch (Throwable throwable) {
				throwable.printStackTrace();
			}
		}
		MethodSignature methodSignature = (MethodSignature) signature;
		Method targetMethod = methodSignature.getMethod();
		// 获取注解
		SOALockService soaLockService = targetMethod.getAnnotation(SOALockService.class);
		// 若没有获取到此注解，则不拦截执行
		if(soaLockService == null){
			try {
				return point.proceed();
			} catch (Throwable throwable) {
				throwable.printStackTrace();
			}
		}
		final String nameSpace = soaLockService.nameSpace();
		Class<?>[] parameterTypes = targetMethod.getParameterTypes();
		StringBuilder paramsName = new StringBuilder();
		if(parameterTypes.length > 0){
			for(int i=0,length = parameterTypes.length;i<length;i++){
				if(i <= length-1){
					paramsName.append(parameterTypes[i].getName()+"-");
				}else{
					paramsName.append(parameterTypes[i].getName());
				}
			}
		}
		final String key = targetMethod.getName() + "-"+paramsName.toString();
		final int expire = soaLockService.expire();
		int waitSOALockTime = soaLockService.waitSOALockTime();

		final boolean requireLock = requireLock(nameSpace, key, expire);
		long waitSOAUnLockTime = 0L;
		if(!requireLock){
			// 如果当前线程没有获取到锁而且设置为不等待锁的释放，则直接返回
			if(waitSOALockTime == 0){
				LOGGER.info("other app get the lock,this method do not run");
				System.out.println("["+name+"]等待释放锁资源,任务放弃处理");
				return null;
			}
			// 否在开始等待锁的释放，并计算等待时间
			long startTime = System.currentTimeMillis();
			ExecutorService exec = Executors.newFixedThreadPool(1);

			// 声明等待释放锁任务内容
			Future<Boolean> future = exec.submit(new Callable<Boolean>() {
				@Override
				public Boolean call() throws Exception {
					boolean requireUnLock = requireLock(nameSpace,key,expire);
					while (!requireUnLock){
						requireUnLock = requireLock(nameSpace,key,expire);
					}
					return true;
				}
			});
			try {
				// 等待其它线程释放锁
				future.get(waitSOALockTime, TimeUnit.SECONDS);
				long endTime = System.currentTimeMillis();
				waitSOAUnLockTime = endTime  - startTime;
				System.out.println("已在设置等待时间内获取到锁,等待锁释放时间:"+waitSOALockTime);
			} catch (InterruptedException e) {
				e.printStackTrace();
				return null;
			} catch (ExecutionException e) {
				e.printStackTrace();
				return null;
			} catch (TimeoutException e) {
				long endTime = System.currentTimeMillis();
				waitSOAUnLockTime = endTime  - startTime;
				future.cancel(true);
				LOGGER.info("this method wait to  th soa lock to release,but timeout");
				System.out.println("["+name+"] 一直在等待锁资源释放,但是超时了，总共等待了["+waitSOAUnLockTime+"ms],此任务已放弃处理");

				return null;
			}finally {
				exec.shutdown();
				releaseLock(nameSpace,key);
			}
		}
		// 表明当前线程是如何获取锁的，方便调试
		if(waitSOALockTime == 0){
			System.out.println("["+name+"]成功获取锁资源，进行下一步处理");
		}else{
			System.out.println("["+name+"]等待"+waitSOAUnLockTime + "ms后,成功获取锁资源,进行下一步处理");
		}

		// 获取到锁后,开始处理自己的任务,并开始任务处理超时监控
		ExecutorService exec = Executors.newFixedThreadPool(1);
		Future<Object> future = exec.submit(new Callable<Object>() {
			@Override
			public Object call() throws Exception {
				try {
					return point.proceed();
				} catch (Throwable throwable) {
					throwable.printStackTrace();
				}
				return null;
			}
		});
		try {
			resultObject = future.get(expire, TimeUnit.SECONDS);
			System.out.println("["+name+"]处理完毕,释放锁资源...");
		} catch (InterruptedException e) {
			e.printStackTrace();
			LOGGER.info("this method run throw interruptedException",e);
			return null;
		} catch (ExecutionException e) {
			e.printStackTrace();
			LOGGER.info("this method run throw executionException",e);
			return null;
		} catch (TimeoutException e) {
			LOGGER.info("this method run timeout",e);
			System.out.println("["+name+"]任务处理超时,释放锁资源...");
			return null;
		}finally {
			future.cancel(true);
			releaseLock(nameSpace,key);
			exec.shutdown();
		}
		return resultObject;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public Integer getPort() {
		return port;
	}

	public void setPort(Integer port) {
		this.port = port;
	}

	public Integer getDatabase() {
		return database;
	}

	public void setDatabase(Integer database) {
		this.database = database;
	}

	public IRedisObjectConverter getRedisObjectConverter() {
		return redisObjectConverter;
	}

	public void setRedisObjectConverter(IRedisObjectConverter redisObjectConverter) {
		this.redisObjectConverter = redisObjectConverter;
	}
	
}

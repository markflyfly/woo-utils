package com.pugwoo.wooutils.redis.anatation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.METHOD,ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface SOALockService {

    String nameSpace();

    /**
     * 任务处理超时时间，超时自动释放锁资源
     * @return
     */
    int expire() default 30;

    /**
     * 等待sao lock释放锁的资源
     * @return
     */
    int waitSOALockTime() default 0;
}

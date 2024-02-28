package com.redislabs.university.RU102J.dao;

import com.redislabs.university.RU102J.core.KeyHelper;
import redis.clients.jedis.*;
import java.time.ZonedDateTime;

public class RateLimiterSlidingDaoRedisImpl implements RateLimiter {

    private final JedisPool jedisPool;
    private final long windowSizeMS;
    private final long maxHits;

    public RateLimiterSlidingDaoRedisImpl(JedisPool pool, long windowSizeMS,
                                          long maxHits) {
        this.jedisPool = pool;
        this.windowSizeMS = windowSizeMS;
        this.maxHits = maxHits;
    }

    // Challenge #7
    @Override
    public void hit(String name) throws RateLimitExceededException {
        // START CHALLENGE #7
        try (Jedis jedis = jedisPool.getResource()) {
            String key = KeyHelper.getKey("limiter:" + windowSizeMS + ":" + name + ":" + maxHits);
            long currentTimestamp = ZonedDateTime.now().toInstant().toEpochMilli();

            String entry = currentTimestamp + "-" + Math.random();

            Transaction transaction = jedis.multi();
            transaction.zadd(key, currentTimestamp, entry);
            transaction.zremrangeByScore(key, 0, currentTimestamp - windowSizeMS);
            Response<Long> hits = transaction.zcard(key);
            transaction.exec();

            if(hits.get() > maxHits) throw new RateLimitExceededException();
        }
        // END CHALLENGE #7
    }
}

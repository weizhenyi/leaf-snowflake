package utils;

/**
 * Created by weizhenyi on 2017/6/25.
 */
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
public class LeafBoundedExponentialBackoffRetry extends BoundedExponentialBackoffRetry {
	private static final Logger LOG = LoggerFactory.getLogger(LeafBoundedExponentialBackoffRetry.class);
	private int stepSize;
	private int expRetriesThreshold;
	private final Random random = new Random();
	private final int linearBaseSleepMs;

	public LeafBoundedExponentialBackoffRetry(int baseSleepTimeMs, int maxSleepTimeMs, int maxRetries)
	{
		super(baseSleepTimeMs, maxSleepTimeMs, maxRetries);
		expRetriesThreshold = 1;
		while ( (1 << (expRetriesThreshold + 1)) < ((maxSleepTimeMs - baseSleepTimeMs) /2 ))
		{
			expRetriesThreshold ++ ;
		}
		LOG.info("The baseSleepTimeMs [" + baseSleepTimeMs + "] the maxSleepTimeMs [" + maxSleepTimeMs +"]" + "the maxRetries [" + maxRetries +"]" );
		if (baseSleepTimeMs > maxSleepTimeMs)
		{
			LOG.warn("Misconfiguration: the baseSleepTimeMs [" + baseSleepTimeMs +"] can not be greater than the maxSleepTimeMs [" + maxSleepTimeMs +"]");
			if (maxRetries > 0 && maxRetries > expRetriesThreshold)
			{
				this.stepSize = Math.max(1 ,(maxSleepTimeMs - (1 << expRetriesThreshold))/ (maxRetries - expRetriesThreshold));
			}
			else
			{
				this.stepSize = 1;
			}
		}
		this.linearBaseSleepMs = super.getBaseSleepTimeMs() + (1 << expRetriesThreshold);
	}

	@Override
	protected int getSleepTimeMs(int retryCount, long elapsedTimeMs) {
		if (retryCount < expRetriesThreshold)
		{
			int exp = 1 << retryCount;
			int jitter = random.nextInt(exp);
			int sleepTimeMs = super.getBaseSleepTimeMs() + exp + jitter;
			return sleepTimeMs;
		}
		else
		{
			int stepJitter = random.nextInt(stepSize);
			return Math.min(super.getMaxSleepTimeMs(), (linearBaseSleepMs + (stepSize * (retryCount - expRetriesThreshold)) + stepJitter));
		}
	}
}

/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest;

import org.opensearch.indexdigest.bucket.BucketMath;
import org.opensearch.indexdigest.bucket.BucketRange;
import org.opensearch.test.OpenSearchTestCase;

public class BucketMathTests extends OpenSearchTestCase {

    public void testBucketSize() {
        BucketMath math = BucketMath.INSTANCE;

        assertEquals(1L, math.bucketSize(0));
        assertEquals(1024L, math.bucketSize(10));
        assertEquals(4096L, math.bucketSize(12));
    }

    public void testBucketIdBoundaries() {
        BucketMath math = BucketMath.INSTANCE;

        assertEquals(0L, math.bucketId(0L, 10));
        assertEquals(0L, math.bucketId(1023L, 10));
        assertEquals(1024L, math.bucketId(1024L, 10));
        assertEquals(1024L, math.bucketId(2047L, 10));
        assertEquals(2048L, math.bucketId(2048L, 10));
        assertEquals(3072L, math.bucketId(4095L, 10));

        assertEquals(0L, math.bucketId(4095L, 12));
        assertEquals(4096L, math.bucketId(4096L, 12));
    }

    public void testNextPrevBucket() {
        BucketMath math = BucketMath.INSTANCE;

        assertEquals(1024L, math.nextBucket(0L, 10));
        assertEquals(2048L, math.nextBucket(1024L, 10));

        assertEquals(0L, math.prevBucket(1024L, 10));
        assertEquals(1024L, math.prevBucket(2048L, 10));
    }

    public void testGetBucketBoundariesSameLevel() {
        BucketMath math = BucketMath.INSTANCE;

        BucketRange range = math.getBucketBoundaries(4096L, 12, 12);

        assertEquals(4096L, range.startBucket());
        assertEquals(4096L, range.endBucket());
    }

    public void testGetBucketBoundariesFromL12ToL10() {
        BucketMath math = BucketMath.INSTANCE;

        BucketRange first = math.getBucketBoundaries(0L, 12, 10);
        assertEquals(0L, first.startBucket());
        assertEquals(3072L, first.endBucket());

        BucketRange second = math.getBucketBoundaries(4096L, 12, 10);
        assertEquals(4096L, second.startBucket());
        assertEquals(7168L, second.endBucket());
    }

    public void testGetBucketBoundariesFromL11ToL10() {
        BucketMath math = BucketMath.INSTANCE;

        BucketRange first = math.getBucketBoundaries(0L, 11, 10);
        assertEquals(0L, first.startBucket());
        assertEquals(1024L, first.endBucket());

        BucketRange second = math.getBucketBoundaries(2048L, 11, 10);
        assertEquals(2048L, second.startBucket());
        assertEquals(3072L, second.endBucket());
    }

    public void testRejectTargetLevelHigherThanBucketLevel() {
        BucketMath math = BucketMath.INSTANCE;

        IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> math.getBucketBoundaries(0L, 10, 12)
        );

        assertTrue(e.getMessage().contains("targetLevel cannot be higher than bucketLevel"));
    }

    public void testRejectLevel63() {
        BucketMath math = BucketMath.INSTANCE;

        IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> math.bucketSize(63)
        );

        assertTrue(e.getMessage().contains("[0, 63)"));
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest.scan;

import org.opensearch.indexdigest.bucket.BucketMath;
import org.opensearch.indexdigest.model.ProgressDoc;
import org.opensearch.test.OpenSearchTestCase;

public class SealedLeafRunPlannerTests extends OpenSearchTestCase {

    public void testBootstrapStartsAtFirstObservedBucket() {
        SealedLeafRun run = planner(10, 0).plan(progress(-1L), 5000L, 9500L);

        assertFalse(run.isEmpty());
        assertEquals(4096L, run.startLeafBucketInclusive);
        assertEquals(7168L, run.endLeafBucketInclusive);
    }

    public void testPartialTopWindowIsNotPlanned() {
        SealedLeafRun run = planner(10, 0).plan(progress(-1L), 4096L, 8000L);

        assertTrue(run.isEmpty());
    }

    public void testProgressInsideTopWindowRebuildsContainingTopWindow() {
        SealedLeafRun run = planner(10, 0).plan(progress(2048L), 0L, 9500L);

        assertFalse(run.isEmpty());
        assertEquals(0L, run.startLeafBucketInclusive);
        assertEquals(7168L, run.endLeafBucketInclusive);
    }

    public void testGuardWindowHoldsBackNewestSealedTopWindow() {
        SealedLeafRun run = planner(10, 1).plan(progress(-1L), 0L, 12288L);

        assertFalse(run.isEmpty());
        assertEquals(0L, run.startLeafBucketInclusive);
        assertEquals(7168L, run.endLeafBucketInclusive);
    }

    public void testMaxTopBucketsPerRunCapsCompleteTopWindows() {
        SealedLeafRun run = planner(2, 0).plan(progress(-1L), 0L, 20000L);

        assertFalse(run.isEmpty());
        assertEquals(0L, run.startLeafBucketInclusive);
        assertEquals(7168L, run.endLeafBucketInclusive);
    }

    private static SealedLeafRunPlanner planner(int maxTopBucketsPerRun, int guardWindows) {
        return new SealedLeafRunPlanner(
                BucketMath.INSTANCE,
                10,
                12,
                maxTopBucketsPerRun,
                guardWindows
        );
    }

    private static ProgressDoc progress(long lastSealedLeafBucketId) {
        return new ProgressDoc(
                "progress",
                "index-uuid",
                0,
                "alloc-1",
                0L,
                lastSealedLeafBucketId
        );
    }
}

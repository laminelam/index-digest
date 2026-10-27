/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest.scan;

import org.opensearch.test.OpenSearchTestCase;

public class ScanSchedulerTests extends OpenSearchTestCase {

    public void testDelayToNextBoundary() {
        long interval = 900_000L; // 15 minutes

        // 10:07:30 -> 7.5 minutes until 10:15
        assertEquals(450_000L, ScanScheduler.delayToNextBoundaryMillis(boundary(10, 7, 30), interval));
        // exactly on a boundary -> schedule the NEXT one, never 0
        assertEquals(interval, ScanScheduler.delayToNextBoundaryMillis(boundary(10, 15, 0), interval));
        // one millisecond after a boundary
        assertEquals(interval - 1, ScanScheduler.delayToNextBoundaryMillis(boundary(10, 15, 0) + 1, interval));

        // Alignment property: two nodes with skewed start times land on the SAME
        // wall-clock boundary.
        long nodeA = boundary(10, 3, 12);
        long nodeB = boundary(10, 11, 47);
        assertEquals(
                nodeA + ScanScheduler.delayToNextBoundaryMillis(nodeA, interval),
                nodeB + ScanScheduler.delayToNextBoundaryMillis(nodeB, interval)
        );
    }

    private static long boundary(int hour, int minute, int second) {
        return ((hour * 60L + minute) * 60L + second) * 1000L;
    }
}

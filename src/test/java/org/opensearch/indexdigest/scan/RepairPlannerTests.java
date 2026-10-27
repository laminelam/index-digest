/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest.scan;

import org.opensearch.indexdigest.bucket.BucketMath;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Set;

/**
 * The repair-scope contract: damage behind the sealed watermark is repaired by
 * re-sealing the affected windows only. On a long-lived index, the cost of a
 * repair must track the span of the damage on the version axis, never the size
 * or age of the index.
 */
public class RepairPlannerTests extends OpenSearchTestCase {

    private static final int MIN_LEVEL = 10;                 // leaf spans 1024
    private static final int MAX_LEVEL = 20;                 // window spans 1 048 576
    private static final long WINDOW = 1L << MAX_LEVEL;
    private static final long LEAF = 1L << MIN_LEVEL;

    private final RepairPlanner planner = new RepairPlanner(BucketMath.INSTANCE, MIN_LEVEL, MAX_LEVEL);

    // ---------------------------------------------------------------- late arrivals

    public void testLateVersionSelectsOnlyItsOwnWindow() {
        long watermark = 50 * WINDOW;                        // windows 0..49 sealed
        long lateVersion = 3 * WINDOW + 5_000;               // lands in window 3

        Set<Long> windows = planner.windowsNeedingReseal(List.of(lateVersion), watermark);

        assertEquals("exactly one window is damaged", Set.of(3 * WINDOW), windows);
    }

    public void testVersionsAboveTheWatermarkAreNotDamage() {
        long watermark = 10 * WINDOW;
        // Ahead of the watermark: ordinary new data, the sealing path owns it.
        Set<Long> windows = planner.windowsNeedingReseal(
                List.of(10 * WINDOW + LEAF * 2, 12 * WINDOW, 40 * WINDOW), watermark);

        assertTrue("nothing above the watermark needs repair: " + windows, windows.isEmpty());
    }

    public void testWatermarkBoundaryIsInclusive() {
        // The leaf bucket AT the watermark is sealed, so a version inside it is late.
        long watermark = 4 * WINDOW + 7 * LEAF;
        long insideSealedLeaf = watermark + 5;               // same leaf bucket
        long firstUnsealed = watermark + LEAF;               // next leaf bucket

        assertEquals(Set.of(4 * WINDOW), planner.windowsNeedingReseal(List.of(insideSealedLeaf), watermark));
        assertTrue(planner.windowsNeedingReseal(List.of(firstUnsealed), watermark).isEmpty());
    }

    public void testManyLateVersionsCollapseToDistinctWindows() {
        long watermark = 50 * WINDOW;
        // Six versions, but only three distinct windows: repair cost is bounded
        // by damaged WINDOWS, not by the number of offending documents.
        Set<Long> windows = planner.windowsNeedingReseal(
                List.of(
                        2 * WINDOW + 10, 2 * WINDOW + 2_000, 2 * WINDOW + 900_000,
                        7 * WINDOW + 1, 7 * WINDOW + 500_000,
                        31 * WINDOW + 42
                ),
                watermark);

        assertEquals(Set.of(2 * WINDOW, 7 * WINDOW, 31 * WINDOW), windows);
    }

    public void testAncientLateVersionDoesNotWidenTheRepair() {
        // A straggler from the very beginning of a long-lived index repairs
        // window 0 alone; windows 1..999 stay sealed.
        long watermark = 1000 * WINDOW;

        Set<Long> windows = planner.windowsNeedingReseal(List.of(123L), watermark);

        assertEquals(Set.of(0L), windows);
        assertEquals(1, windows.size());
    }

    public void testNothingSealedYetMeansNothingToRepair() {
        assertTrue(planner.windowsNeedingReseal(List.of(5L, 5 * WINDOW), -1L).isEmpty());
    }

    public void testMalformedVersionsAreIgnoredNotRepaired() {
        long watermark = 10 * WINDOW;
        Set<Long> windows = planner.windowsNeedingReseal(
                java.util.Arrays.asList(null, -7L, 2 * WINDOW + 3), watermark);

        assertEquals("only the valid late version drives a repair", Set.of(2 * WINDOW), windows);
    }

    // ---------------------------------------------------------------- history gap

    public void testHistoryGapRewindsOnlyToTheLastHarvestFrontier() {
        // A year of sealed history; the scanner was down for one window's worth
        // of version space.
        long watermark = 1000 * WINDOW + 500 * LEAF;
        long frontierAtLastHarvest = 998 * WINDOW + 42;

        long rewound = planner.rewindWatermarkForHistoryGap(frontierAtLastHarvest, watermark);

        // Everything below window 998 stays sealed; only 998 onwards is redone.
        assertEquals(998 * WINDOW - LEAF, rewound);
        assertTrue("rewind must move backwards", rewound < watermark);
        long windowsRedone = (watermark - rewound) / WINDOW;
        assertTrue("repair must stay bounded, redid " + windowsRedone + " windows", windowsRedone <= 3);
    }

    public void testHistoryGapNeverRebuildsFromTheBeginning() {
        long watermark = 5000 * WINDOW;
        long frontier = 4999 * WINDOW + 10;

        long rewound = planner.rewindWatermarkForHistoryGap(frontier, watermark);

        assertTrue("must not rewind to the bootstrap sentinel", rewound > 0L);
        assertTrue("must keep the overwhelming majority of history sealed", rewound > 4900 * WINDOW);
    }

    public void testUnknownFrontierRedoesOnlyTheNewestWindow() {
        // Progress written by an older build carries no frontier mark.
        long watermark = 700 * WINDOW + 3 * LEAF;

        long rewound = planner.rewindWatermarkForHistoryGap(-1L, watermark);

        assertEquals("only the newest window is redone", 700 * WINDOW - LEAF, rewound);
    }

    public void testHistoryGapBeforeAnythingSealedIsANoOp() {
        assertEquals(-1L, planner.rewindWatermarkForHistoryGap(5 * WINDOW, -1L));
    }

    public void testRewindIsIdempotent() {
        long watermark = 100 * WINDOW + 9 * LEAF;
        long frontier = 99 * WINDOW + 5;

        long once = planner.rewindWatermarkForHistoryGap(frontier, watermark);
        long twice = planner.rewindWatermarkForHistoryGap(frontier, once);

        assertEquals("repeated gap detection must not walk the watermark backwards", once, twice);
    }

    public void testFrontierInsideTheFirstWindowClampsToSentinel() {
        long watermark = 3 * WINDOW;
        long frontierInWindowZero = 12_345L;

        long rewound = planner.rewindWatermarkForHistoryGap(frontierInWindowZero, watermark);

        assertEquals("rewinding into window 0 means nothing stays sealed", -1L, rewound);
    }
}

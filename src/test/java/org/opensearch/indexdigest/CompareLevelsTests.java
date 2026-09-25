/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest;

import org.opensearch.indexdigest.model.ProgressDoc;
import org.opensearch.test.OpenSearchTestCase;

public class CompareLevelsTests extends OpenSearchTestCase {

    private static final int NODE_MIN = 10;
    private static final int NODE_MAX = 20;

    public void testBothMissingFallsBackWithoutIncompatibility() {
        IndexDigestService.CompareLevels levels =
                IndexDigestService.resolveCompareLevels(null, null, NODE_MIN, NODE_MAX);

        assertFalse(levels.incompatible);
        assertEquals(NODE_MIN, levels.minLevel);
        assertEquals(NODE_MAX, levels.maxLevel);
    }

    public void testOneMissingFallsBackWithoutIncompatibility() {
        ProgressDoc found = progress(12, 22);

        IndexDigestService.CompareLevels levels =
                IndexDigestService.resolveCompareLevels(found, null, NODE_MIN, NODE_MAX);

        assertFalse(levels.incompatible);
        assertEquals(NODE_MIN, levels.minLevel);
        assertEquals(NODE_MAX, levels.maxLevel);
    }

    public void testMatchingSidesUseTheirOwnLevelsNotTheNodeDefaults() {
        IndexDigestService.CompareLevels levels =
                IndexDigestService.resolveCompareLevels(progress(12, 22), progress(12, 22), NODE_MIN, NODE_MAX);

        assertFalse(levels.incompatible);
        assertEquals(12, levels.minLevel);
        assertEquals(22, levels.maxLevel);
    }

    public void testMismatchedSidesAreIncompatible() {
        IndexDigestService.CompareLevels levels =
                IndexDigestService.resolveCompareLevels(progress(10, 20), progress(12, 22), NODE_MIN, NODE_MAX);

        assertTrue(levels.incompatible);
    }

    public void testLegacyLevelsAreIncompatible() {
        IndexDigestService.CompareLevels left =
                IndexDigestService.resolveCompareLevels(progress(-1, -1), progress(10, 20), NODE_MIN, NODE_MAX);
        IndexDigestService.CompareLevels right =
                IndexDigestService.resolveCompareLevels(progress(10, 20), progress(-1, -1), NODE_MIN, NODE_MAX);
        IndexDigestService.CompareLevels both =
                IndexDigestService.resolveCompareLevels(progress(-1, -1), progress(-1, -1), NODE_MIN, NODE_MAX);

        assertTrue(left.incompatible);
        assertTrue(right.incompatible);
        // Even matching legacy levels are incompatible: their digests were built
        // under an unknown configuration.
        assertTrue(both.incompatible);
    }

    public void testMismatchedFormatVersionsAreIncompatible() {
        IndexDigestService.CompareLevels levels = IndexDigestService.resolveCompareLevels(
                progress(10, 20, 2),
                progress(10, 20, 3),
                NODE_MIN,
                NODE_MAX
        );
        assertTrue(levels.incompatible);

        IndexDigestService.CompareLevels sameNonCurrent = IndexDigestService.resolveCompareLevels(
                progress(10, 20, 3),
                progress(10, 20, 3),
                NODE_MIN,
                NODE_MAX
        );
        // Side-vs-side principle: matching formats compare, whatever this
        // coordinating node runs.
        assertFalse(sameNonCurrent.incompatible);
    }

    private static ProgressDoc progress(int minLevel, int maxLevel) {
        return progress(minLevel, maxLevel, 2);
    }

    private static ProgressDoc progress(int minLevel, int maxLevel, int formatVersion) {
        return new ProgressDoc(
                "p:uuid:0:alloc",
                "uuid",
                0,
                "alloc",
                "node",
                "primary",
                "primary_only",
                minLevel,
                maxLevel,
                123L,
                1024L,
                -1L,
                formatVersion,
                -1L
        );
    }
}

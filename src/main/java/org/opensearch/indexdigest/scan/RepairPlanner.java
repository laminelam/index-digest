/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest.scan;

import org.opensearch.indexdigest.bucket.BucketMath;

import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

/**
 * Decides the SCOPE of a repair when something lands behind the sealed
 * watermark.
 *
 * The governing property is that the external version is monotonic: anything
 * that arrives late, or that was missed while the scanner was down,
 * necessarily carries an external version at or above the point where things
 * last went right.
 * Everything below that point was sealed correctly and must never be rebuilt
 * — on a long-lived index that would mean re-hashing years of data to repair
 * minutes of damage.
 */
public final class RepairPlanner {

    private final BucketMath bucketMath;
    private final int minLevel;
    private final int maxLevel;

    public RepairPlanner(BucketMath bucketMath, int minLevel, int maxLevel) {
        this.bucketMath = bucketMath;
        this.minLevel = minLevel;
        this.maxLevel = maxLevel;
    }

    /**
     * Top windows to reseal for external versions that landed at or below the
     * sealed watermark. External versions above the watermark are not damage:
     * the normal sealing path has not reached them yet and will fold them in
     * due course.
     */
    public Set<Long> windowsNeedingReseal(Collection<Long> externalVersions, long sealedWatermark) {
        Set<Long> windows = new TreeSet<>();
        if (externalVersions == null || sealedWatermark < 0L) {
            return windows;
        }
        for (Long externalVersion : externalVersions) {
            if (externalVersion == null || externalVersion < 0L) {
                continue;
            }
            if (bucketMath.bucketId(externalVersion, minLevel) <= sealedWatermark) {
                windows.add(bucketMath.bucketId(externalVersion, maxLevel));
            }
        }
        return windows;
    }

    /**
     * Where to rewind the sealed watermark after a tombstone-history gap.
     *
     * Deletes lost during the gap were issued after the last successful
     * harvest, so their external versions sit at or above {@code lastHarvestMaxExternalVersion}.
     * Re-sealing from the start of that frontier's window is therefore
     * sufficient, and the cost is bounded by the outage's span on the version
     * axis rather than by the size of the index.
     *
     * Returns the new watermark: the last leaf bucket that stays sealed.
     */
    public long rewindWatermarkForHistoryGap(long lastHarvestMaxExternalVersion, long currentWatermark) {
        if (currentWatermark < 0L) {
            // Nothing sealed yet; the ordinary bootstrap path covers it.
            return currentWatermark;
        }
        if (lastHarvestMaxExternalVersion < 0L) {
            // Frontier unknown (e.g. progress written by an older build). Redo
            // the newest window only: still bounded, and the compare/reseal
            // loop remains available for anything older that turns out wrong.
            long newestWindow = bucketMath.bucketId(currentWatermark, maxLevel);
            return clampBelowWindow(newestWindow, currentWatermark);
        }
        long frontierWindow = bucketMath.bucketId(lastHarvestMaxExternalVersion, maxLevel);
        return clampBelowWindow(frontierWindow, currentWatermark);
    }

    /**
     * The last leaf bucket strictly below {@code window}, never above the
     * current watermark (a rewind only ever moves backwards) and never below
     * -1 (the "nothing sealed" sentinel).
     */
    private long clampBelowWindow(long window, long currentWatermark) {
        long lastSealedBelow = window <= 0L ? -1L : bucketMath.prevBucket(window, minLevel);
        if (lastSealedBelow < -1L) {
            lastSealedBelow = -1L;
        }
        return Math.min(lastSealedBelow, currentWatermark);
    }
}

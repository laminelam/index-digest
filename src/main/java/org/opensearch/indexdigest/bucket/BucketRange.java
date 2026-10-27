/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest.bucket;

/**
 * Represents a bucket range [startBucket, endBucket].
 */
public record BucketRange(long startBucket, long endBucket) {
}

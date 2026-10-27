/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest.persistence;

import org.opensearch.indices.SystemIndexDescriptor;

/**
 * System index descriptor for Hierarchical Index Digest.
 * <p>
 * Note: the DAO also creates the index in v1 (brick-by-brick). In later iterations,
 * you may rely purely on SystemIndexPlugin + templates / migrations.
 */
public final class IndexDigestSystemIndex {
    private IndexDigestSystemIndex() {
    }

    public static final String INDEX_NAME = ".index_digest";
    public static final String DESCRIPTION = "System index used by the Hierarchical Index Digest plugin";

    public static SystemIndexDescriptor descriptor() {
        return new SystemIndexDescriptor(INDEX_NAME, DESCRIPTION);
    }
}

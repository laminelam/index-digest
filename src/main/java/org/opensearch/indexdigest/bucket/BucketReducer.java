/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest.bucket;

import org.opensearch.indexdigest.model.BucketDoc;
import org.opensearch.indexdigest.model.ShardCopyKey;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;

public final class BucketReducer {
    public static final String DIGEST_ALGO = "SHA-256";
    private static final byte[] FORMAT_MAGIC = "INDEX_DIGEST_PARENT_V2"
                                                .getBytes(StandardCharsets.UTF_8);

    private final BucketMath bucketMath;

    public BucketReducer(BucketMath bucketMath) {
        this.bucketMath = bucketMath;
    }

    public BucketDoc reduce(
            ShardCopyKey key,
            int parentLevel,
            long parentBucketId,
            List<BucketDoc> children,
            long sealedByMaxExternalVersion
    ) {
        if (children.isEmpty()) {
            throw new IllegalArgumentException("children must not be empty");
        }

        List<BucketDoc> ordered = children.stream()
                .sorted(Comparator.comparingLong(d -> d.bucketId))
                .toList();

        long docCount = 0L;
        long deleteEventCount = 0L;
        for (BucketDoc child : ordered) {
            docCount += child.docCount;
            deleteEventCount += child.deleteEventCount;
        }

        String digest = computeDigest(parentLevel, parentBucketId, ordered);

        return BucketDoc.parent(
                key,
                parentLevel,
                parentBucketId,
                parentBucketId,
                bucketMath.nextBucket(parentBucketId, parentLevel),
                digest,
                docCount,
                ordered.size(),
                deleteEventCount,
                sealedByMaxExternalVersion
        );
    }

    private String computeDigest(int level, long bucketId, List<BucketDoc> children) {
        try {
            MessageDigest md = MessageDigest.getInstance(DIGEST_ALGO);
            md.update(FORMAT_MAGIC);
            md.update(intBytes(level));
            md.update(longBytes(bucketId));
            md.update(intBytes(children.size()));

            for (BucketDoc child : children) {
                md.update(encodeChild(child));
            }

            return HexFormat.of().formatHex(md.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Missing digest algorithm " + DIGEST_ALGO, e);
        }
    }

    private byte[] encodeChild(BucketDoc child) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);

            out.writeLong(child.bucketId);
            out.writeInt(child.level);

            byte[] digestBytes;
            try {
                digestBytes = HexFormat.of().parseHex(child.digest);
            } catch (RuntimeException e) {
                throw new IllegalArgumentException(
                        "Invalid hex digest for child bucket: level=" + child.level
                                + ", bucketId=" + child.bucketId
                                + ", digest=" + child.digest,
                        e
                );
            }

            out.writeInt(digestBytes.length);
            out.write(digestBytes);

            out.writeLong(child.docCount);

            out.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed encoding child bucket summary", e);
        }
    }

    private static byte[] longBytes(long v) {
        return new byte[] {
                (byte) (v >>> 56),
                (byte) (v >>> 48),
                (byte) (v >>> 40),
                (byte) (v >>> 32),
                (byte) (v >>> 24),
                (byte) (v >>> 16),
                (byte) (v >>> 8),
                (byte) v
        };
    }

    private static byte[] intBytes(int v) {
        return new byte[] {
                (byte) (v >>> 24),
                (byte) (v >>> 16),
                (byte) (v >>> 8),
                (byte) v
        };
    }
}

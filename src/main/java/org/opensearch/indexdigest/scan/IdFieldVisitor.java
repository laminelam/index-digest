/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest.scan;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.opensearch.index.mapper.Uid;

import java.io.IOException;

public final class IdFieldVisitor extends StoredFieldVisitor {

    private String id;

    @Override
    public Status needsField(FieldInfo fieldInfo) {
        if (id != null) {
            return Status.STOP;
        }
        if ("_id".equals(fieldInfo.name)) {
            return Status.YES;
        }
        return Status.NO;
    }

    @Override
    public void stringField(FieldInfo fieldInfo, String value) throws IOException {
        id = value;
    }

    @Override
    public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
        decodeId(value);
    }

    private void decodeId(byte[] value) {
        try {
            id = Uid.decodeId(value);
        } catch (Exception e) {
            throw new IllegalStateException("Failed decoding stored [_id]", e);
        }
    }

    public String getId() {
        return id;
    }

}

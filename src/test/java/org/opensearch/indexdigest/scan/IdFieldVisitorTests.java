/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest.scan;

import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.StoredFieldDataInput;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.mapper.Uid;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

public class IdFieldVisitorTests extends OpenSearchTestCase {

    public void testByteArrayBinaryFieldDecodesUid() throws IOException {
        IdFieldVisitor visitor = new IdFieldVisitor();

        visitor.binaryField(idFieldInfo(), encodeId("doc-123"));

        assertEquals("doc-123", visitor.getId());
    }

    public void testStoredFieldDataInputDecodesUidEncodingsAndConsumesExactlyTheField() throws IOException {
        for (String id : new String[] { "1234", "AQIDBA", "doc:123" }) {
            byte[] encodedId = encodeId(id);
            int prefixLength = 2;
            byte[] storedFields = new byte[prefixLength + encodedId.length + 2];
            Arrays.fill(storedFields, (byte) 0x7f);
            System.arraycopy(encodedId, 0, storedFields, prefixLength, encodedId.length);

            ByteArrayDataInput input = new ByteArrayDataInput(storedFields);
            input.setPosition(prefixLength);
            IdFieldVisitor visitor = new IdFieldVisitor();

            visitor.binaryField(idFieldInfo(), new StoredFieldDataInput(input, encodedId.length));

            assertEquals("ID should round-trip through StoredFieldDataInput", id, visitor.getId());
            assertEquals("visitor must consume exactly the stored ID", prefixLength + encodedId.length, input.getPosition());
        }
    }

    public void testNeedsFieldStopsAfterIdIsLoaded() throws IOException {
        IdFieldVisitor visitor = new IdFieldVisitor();

        assertEquals(StoredFieldVisitor.Status.NO, visitor.needsField(fieldInfo("_source")));
        assertEquals(StoredFieldVisitor.Status.YES, visitor.needsField(idFieldInfo()));

        visitor.binaryField(idFieldInfo(), encodeId("doc:123"));

        assertEquals(StoredFieldVisitor.Status.STOP, visitor.needsField(fieldInfo("_source")));
        assertEquals(StoredFieldVisitor.Status.STOP, visitor.needsField(idFieldInfo()));
    }

    private static FieldInfo idFieldInfo() {
        return fieldInfo("_id");
    }

    private static FieldInfo fieldInfo(String name) {
        return new FieldInfo(
                name,
                1,
                false,
                false,
                true,
                IndexOptions.NONE,
                DocValuesType.NONE,
                DocValuesSkipIndexType.NONE,
                -1L,
                Collections.emptyMap(),
                0,
                0,
                0,
                0,
                VectorEncoding.FLOAT32,
                VectorSimilarityFunction.EUCLIDEAN,
                false,
                false
        );
    }

    private static byte[] encodeId(String id) {
        BytesRef encoded = Uid.encodeId(id);
        return Arrays.copyOfRange(encoded.bytes, encoded.offset, encoded.offset + encoded.length);
    }
}

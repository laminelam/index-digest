/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest.persistence;

import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.indexdigest.client.PluginClient;
import org.opensearch.indexdigest.model.ShardCopyKey;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.AdminClient;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.IndicesAdminClient;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IndexDigestIndexDaoTests extends OpenSearchTestCase {

    public void testCanonicalMappingPassesVerification() throws Exception {
        IndexDigestIndexDao.verifyMappingProperties(canonicalMappingSource());
    }

    public void testMissingFieldIsDrift() throws Exception {
        Map<String, Object> mappingSource = canonicalMappingSource();
        properties(mappingSource).remove("min_level");

        IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> IndexDigestIndexDao.verifyMappingProperties(mappingSource)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("missing field [min_level]"));
    }

    public void testDynamicTextFieldIsDrift() throws Exception {
        // Shape produced by dynamic mapping when a string field is auto-mapped:
        // text with a .keyword multi-field, instead of the canonical keyword.
        Map<String, Object> mappingSource = canonicalMappingSource();
        Map<String, Object> dynamicText = new HashMap<>();
        dynamicText.put("type", "text");
        dynamicText.put("fields", Map.of("keyword", Map.of("type", "keyword", "ignore_above", 256)));
        properties(mappingSource).put("allocation_id", dynamicText);

        IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> IndexDigestIndexDao.verifyMappingProperties(mappingSource)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("[allocation_id]"));
        assertTrue(e.getMessage(), e.getMessage().contains("expected type [keyword] but found [text]"));
    }

    public void testDynamicLongFieldIsDrift() throws Exception {
        // Dynamic mapping types whole JSON numbers as long, not integer.
        Map<String, Object> mappingSource = canonicalMappingSource();
        properties(mappingSource).put("min_level", Map.of("type", "long"));

        IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> IndexDigestIndexDao.verifyMappingProperties(mappingSource)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("[min_level]"));
        assertTrue(e.getMessage(), e.getMessage().contains("expected type [integer] but found [long]"));
    }

    public void testMissingPropertiesIsDrift() {
        IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> IndexDigestIndexDao.verifyMappingProperties(Map.of())
        );
        assertTrue(e.getMessage(), e.getMessage().contains("missing properties"));
    }

    public void testBulkFailurePreservesTypedCause() {
        IndexNotFoundException cause = new IndexNotFoundException(IndexDigestSystemIndex.INDEX_NAME);
        BulkItemResponse failed = new BulkItemResponse(
                0,
                DocWriteRequest.OpType.INDEX,
                new BulkItemResponse.Failure(IndexDigestSystemIndex.INDEX_NAME, "b:doc", cause)
        );
        BulkResponse response = new BulkResponse(new BulkItemResponse[] { failed }, 1L);

        RuntimeException e = expectThrows(
                RuntimeException.class,
                () -> IndexDigestIndexDao.throwIfBulkFailures(response)
        );

        boolean foundIndexNotFound = false;
        for (Throwable cur = e; cur != null; cur = cur.getCause()) {
            if (cur instanceof IndexNotFoundException) {
                foundIndexNotFound = true;
                break;
            }
        }
        assertTrue("IndexNotFoundException must stay reachable in the cause chain", foundIndexNotFound);
    }

    public void testBulkSuccessDoesNotThrow() {
        BulkResponse response = new BulkResponse(new BulkItemResponse[0], 1L);
        IndexDigestIndexDao.throwIfBulkFailures(response);
    }

    @SuppressWarnings("unchecked")
    public void testRetriesWhenSystemIndexDisappearsDuringInitialEnsure() {
        Client client = mock(Client.class);
        AdminClient adminClient = mock(AdminClient.class);
        IndicesAdminClient indicesClient = mock(IndicesAdminClient.class);
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.indices()).thenReturn(indicesClient);

        ActionFuture<IndicesExistsResponse> existsFuture = mock(ActionFuture.class);
        when(indicesClient.exists(any())).thenReturn(existsFuture);
        when(existsFuture.actionGet())
                .thenReturn(new IndicesExistsResponse(true), new IndicesExistsResponse(false));

        ActionFuture<AcknowledgedResponse> putMappingFuture = mock(ActionFuture.class);
        when(indicesClient.putMapping(any())).thenReturn(putMappingFuture);

        ActionFuture<GetMappingsResponse> mappingsFuture = mock(ActionFuture.class);
        when(indicesClient.getMappings(any())).thenReturn(mappingsFuture);
        when(mappingsFuture.actionGet()).thenThrow(new IndexNotFoundException(IndexDigestSystemIndex.INDEX_NAME));

        ActionFuture<CreateIndexResponse> createFuture = mock(ActionFuture.class);
        when(indicesClient.create(any())).thenReturn(createFuture);

        GetResponse getResponse = mock(GetResponse.class);
        when(getResponse.isExists()).thenReturn(false);
        ActionFuture<GetResponse> getFuture = mock(ActionFuture.class);
        when(client.get(any())).thenReturn(getFuture);
        when(getFuture.actionGet()).thenReturn(getResponse);

        IndexDigestIndexDao dao = new IndexDigestIndexDao(new PluginClient(client), mock(ThreadPool.class));

        assertNull(dao.getProgress(new ShardCopyKey("index-uuid", 0, "allocation-id")));
        assertTrue(dao.consumeSystemIndexRecreated());
        verify(indicesClient, times(2)).exists(any());
        verify(indicesClient).getMappings(any());
        verify(indicesClient).create(any());
        verify(client).get(any());
    }

    @SuppressWarnings("unchecked")
    public void testInitialEnsurePropagatesAfterSingleRetryIsExhausted() {
        Client client = mock(Client.class);
        AdminClient adminClient = mock(AdminClient.class);
        IndicesAdminClient indicesClient = mock(IndicesAdminClient.class);
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.indices()).thenReturn(indicesClient);

        ActionFuture<IndicesExistsResponse> existsFuture = mock(ActionFuture.class);
        when(indicesClient.exists(any())).thenReturn(existsFuture);
        when(existsFuture.actionGet())
                .thenReturn(new IndicesExistsResponse(true), new IndicesExistsResponse(true));

        ActionFuture<AcknowledgedResponse> putMappingFuture = mock(ActionFuture.class);
        when(indicesClient.putMapping(any())).thenReturn(putMappingFuture);

        ActionFuture<GetMappingsResponse> mappingsFuture = mock(ActionFuture.class);
        when(indicesClient.getMappings(any())).thenReturn(mappingsFuture);
        when(mappingsFuture.actionGet()).thenThrow(
                new IndexNotFoundException(IndexDigestSystemIndex.INDEX_NAME),
                new IndexNotFoundException(IndexDigestSystemIndex.INDEX_NAME)
        );

        IndexDigestIndexDao dao = new IndexDigestIndexDao(new PluginClient(client), mock(ThreadPool.class));

        expectThrows(
                IndexNotFoundException.class,
                () -> dao.getProgress(new ShardCopyKey("index-uuid", 0, "allocation-id"))
        );
        assertFalse(dao.consumeSystemIndexRecreated());
        verify(indicesClient, times(2)).exists(any());
        verify(indicesClient, times(2)).getMappings(any());
        verify(indicesClient, never()).create(any());
        verify(client, never()).get(any());
    }

    private static Map<String, Object> canonicalMappingSource() throws Exception {
        XContentBuilder builder = IndexDigestIndexDao.systemIndexMapping();
        Map<String, Object> immutable = XContentHelper.convertToMap(
                BytesReference.bytes(builder),
                false,
                MediaTypeRegistry.JSON
        ).v2();
        // Deep-copy the two levels the tests mutate.
        Map<String, Object> mappingSource = new HashMap<>(immutable);
        mappingSource.put("properties", new HashMap<>((Map<?, ?>) immutable.get("properties")));
        return mappingSource;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> properties(Map<String, Object> mappingSource) {
        return (Map<String, Object>) mappingSource.get("properties");
    }
}

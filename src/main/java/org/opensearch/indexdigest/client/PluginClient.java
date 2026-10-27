/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest.client;

import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.CheckedSupplier;
import org.opensearch.identity.PluginSubject;
import org.opensearch.transport.client.Client;

/**
 * Minimal subject-aware wrapper around Client.
 *
 * Pattern: keep ONE stable instance in the plugin; assignSubject updates it.
 * DAO and other components rely on this wrapper instead of storing PluginSubject themselves.
 */
public final class PluginClient {
    private final Client client;
    private volatile PluginSubject subject;

    public PluginClient(Client client) {
        this.client = client;
    }

    public Client client() {
        return client;
    }

    public void setSubject(PluginSubject subject) {
        this.subject = subject;
    }

    public void runAs(CheckedRunnable<Exception> r) {
        PluginSubject s = this.subject;
        try {
            if (s != null) {
                s.runAs(r);
            } else {
                // Security not installed or subject not assigned yet.
                r.run();
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public <T> T runAsResult(CheckedSupplier<T, Exception> supplier) {
        PluginSubject s = this.subject;
        try {
            if (s != null) {
                final Object[] result = new Object[1];
                s.runAs(() -> result[0] = supplier.get());
                @SuppressWarnings("unchecked")
                T typed = (T) result[0];
                return typed;
            } else {
                return supplier.get();
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

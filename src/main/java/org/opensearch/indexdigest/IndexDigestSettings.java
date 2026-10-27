/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.indexdigest.scan.ScanMode;

import java.util.List;

public final class IndexDigestSettings {
    private IndexDigestSettings() {}

    public static final Setting<List<String>> TARGET_INDICES =
            Setting.listSetting(
                    "plugins.index_digest.indices",
                    List.of(),
                    s -> s,
                    Property.NodeScope,
                    Property.Dynamic
            );

    public static final Setting<Boolean> ENABLED =
            Setting.boolSetting("plugins.index_digest.enabled", true, Property.NodeScope, Property.Dynamic);

    public static final Setting<TimeValue> SCAN_INTERVAL =
            Setting.timeSetting(
                    "plugins.index_digest.scan_interval",
                    TimeValue.timeValueSeconds(30),
                    TimeValue.timeValueSeconds(1),
                    Property.NodeScope,
                    Property.Dynamic
            );

    public static final Setting<String> SCAN_MODE =
            Setting.simpleString(
                    "plugins.index_digest.scan_mode",
                    ScanMode.PRIMARY_ONLY.settingValue(),
                    value -> {
                        ScanMode.parse(value);
                    },
                    Property.NodeScope,
                    Property.Dynamic
            );

    public static final Setting<List<String>> SELECTED_SHARD_COPIES =
            Setting.listSetting(
                    "plugins.index_digest.selected_shard_copies",
                    List.of(),
                    s -> s,
                    Property.NodeScope,
                    Property.Dynamic
            );

    public static final Setting<Integer> MIN_LEVEL =
            Setting.intSetting("plugins.index_digest.min_level", 10, 0, 62, Property.NodeScope);

    public static final Setting<Integer> MAX_LEVEL =
            Setting.intSetting("plugins.index_digest.max_level", 20, 0, 62, Property.NodeScope);

    public static final Setting<Integer> MAX_TOP_BUCKETS_PER_RUN =
            Setting.intSetting("plugins.index_digest.max_top_buckets_per_run", 1, 1, Property.NodeScope, Property.Dynamic);

    /**
     * When true (default), _compare enqueues automatic reseal requests for
     * mismatched windows (a small, non-destructive write to .index_digest that the
     * owning scanners act on). Set to false to make _compare strictly read-only.
     */
    public static final Setting<Boolean> AUTO_RESEAL =
            Setting.boolSetting("plugins.index_digest.auto_reseal", true, Property.NodeScope, Property.Dynamic);

    public static final Setting<Integer> GUARD_WINDOWS =
            Setting.intSetting("plugins.index_digest.guard_windows", 1, 0, Property.NodeScope, Property.Dynamic);

    public static final List<Setting<?>> ALL_SETTINGS = List.of(
            TARGET_INDICES,
            ENABLED,
            SCAN_INTERVAL,
            SCAN_MODE,
            SELECTED_SHARD_COPIES,
            MIN_LEVEL,
            MAX_LEVEL,
            MAX_TOP_BUCKETS_PER_RUN,
            GUARD_WINDOWS,
            AUTO_RESEAL
    );
}

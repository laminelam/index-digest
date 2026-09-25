/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest.scan;

import java.util.Locale;

public enum ScanMode {
    PRIMARY_ONLY("primary_only"),
    ALL_COPIES("all_copies"),
    SELECTED_COPIES("selected_copies");

    private final String settingValue;

    ScanMode(String settingValue) {
        this.settingValue = settingValue;
    }

    public String settingValue() {
        return settingValue;
    }

    public static ScanMode parse(String value) {
        if (value == null) {
            throw new IllegalArgumentException("scan mode must not be null");
        }

        String normalized = value.trim().toLowerCase(Locale.ROOT);
        for (ScanMode mode : values()) {
            if (mode.settingValue.equals(normalized)) {
                return mode;
            }
        }

        throw new IllegalArgumentException(
                "invalid index-digest scan mode [" + value + "], expected one of ["
                        + PRIMARY_ONLY.settingValue + ", "
                        + ALL_COPIES.settingValue + ", "
                        + SELECTED_COPIES.settingValue + "]"
        );
    }
}

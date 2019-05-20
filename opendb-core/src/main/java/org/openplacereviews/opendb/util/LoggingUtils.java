package org.openplacereviews.opendb.util;

public class LoggingUtils {

    public static StringBuilder buildRequestTimingLogLine(long tookMs) {
        StringBuilder b = new StringBuilder("[").append(tookMs);
        if (tookMs >= 10000) {
            b.append(" ms] OVER_10SEC <-- ");
        } else if (tookMs >= 3000) {
            b.append(" ms] OVER_3SEC <-- ");
        } else if (tookMs >= 1000) {
            b.append(" ms] OVER_1SEC <-- ");
        } else if (tookMs >= 500) {
            b.append(" ms] OVER_500MS <-- ");
        } else if (tookMs >= 300) {
            b.append(" ms] OVER_200MS <-- ");
        } else {
            b.append(" ms] <-- ");
        }
        return b;
    }
}
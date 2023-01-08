package com.github.shawven.calf.track.register;

import com.github.shawven.calf.track.common.Const;
import org.springframework.util.StringUtils;

/**
 * @author xw
 * @date 2023-01-05
 */
public class PathKey {

    private static final String DELIMITER = "/";
    private static String PREFIX;

    static {
        init(Const.PREFIX);
    }

    public static void init(String root) {
        // init
        root = ensureStartSlash(root);
        root = ensureEndSlash(root);
        PREFIX = root;
    }

    private PathKey() {}

    public static String concat(String key) {
        if (!StringUtils.hasText(key)) {
            return PREFIX;
        }

        return PREFIX.concat(removeStartSlash(key));
    }

    public static String concat(String... keys) {
        return concat(String.join(DELIMITER, keys));
    }

    private static String removeStartSlash(String path) {
        return path.startsWith(DELIMITER) ? path.substring(1, path.length() - 1) : path;
    }

    private static String ensureStartSlash(String path) {
        return !path.startsWith(DELIMITER) ? DELIMITER.concat(path) : path;
    }

    private static String ensureEndSlash(String path) {
        return !path.endsWith(DELIMITER) ? path.concat(DELIMITER) : path;
    }
}

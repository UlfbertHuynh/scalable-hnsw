package ai.preferred.cerebro;

import org.apache.commons.lang3.SystemUtils;

public class IndexConst {
    //shortcut for splash, this is due to the difference in
    // directory separator between window os and linux-based os
    public static final char Sp = SystemUtils.IS_OS_WINDOWS ? '\\' : '/';
}
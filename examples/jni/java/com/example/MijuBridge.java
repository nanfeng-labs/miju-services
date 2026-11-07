package com.example;

/** This class itself loads the library and exclusively owns these five declarations. */
public final class MijuBridge {
    static { System.loadLibrary("miju_jni_udp"); }
    private MijuBridge() {}
    public static native int nativeStart(String[] args);
    public static native int nativeStop(long timeoutMillis);
    public static native int nativeStatus();
    public static native String nativeLastError();
    public static native String nativeVersion();
}

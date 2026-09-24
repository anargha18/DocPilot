package com.docpilot.common.storage;

import java.io.InputStream;

public interface StorageService {
    String upload(String filename, byte[] data, String contentType);
    InputStream download(String storageKey);
}

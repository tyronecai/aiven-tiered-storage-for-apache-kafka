/*
 * Copyright 2023 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.tieredstorage.storage.filesystem;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import com.google.common.collect.ImmutableMap;
import io.aiven.kafka.tieredstorage.storage.BaseStorageTest;
import io.aiven.kafka.tieredstorage.storage.StorageBackend;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;
import io.aiven.kafka.tieredstorage.storage.TestObjectKey;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileSystemStorageTest extends BaseStorageTest {

    @TempDir
    Path root;

    @Override
    protected StorageBackend storage() {
        final FileSystemStorage storage = new FileSystemStorage();
        storage.configure(ImmutableMap.of("root", root.toString()));
        return storage;
    }

    @Test
    void testRootCannotBeAFile() throws IOException {
        final Path wrongRoot = root.resolve("file_instead");
        Files.write(wrongRoot, "Wrong root".getBytes(StandardCharsets.UTF_8));

        assertThatThrownBy(() -> {
            final FileSystemStorage storage = new FileSystemStorage();
            storage.configure(ImmutableMap.of("root", wrongRoot.toString()));
        })
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage(wrongRoot + " must be a writable directory");
    }

    @Test
    void testRootCannotBeNonWritableDirectory() throws IOException {
        // 对root用户无效
        if (!"root".equals(System.getProperty("user.name"))) {
            final Path nonWritableDir = root.resolve("non_writable");
            Files.createDirectory(nonWritableDir).toFile().setReadOnly();

            assertThatThrownBy(() -> {
                final FileSystemStorage storage = new FileSystemStorage();
                storage.configure(ImmutableMap.of("root", nonWritableDir.toString()));
            })
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage(nonWritableDir + " must be a writable directory");
        }
    }

    @Test
    void testDeleteAllParentsButRoot() throws IOException, StorageBackendException {
        final Path keyPath = root.resolve(TOPIC_PARTITION_SEGMENT_KEY.value());
        Files.createDirectories(keyPath.getParent());
        Files.write(keyPath, "test".getBytes(StandardCharsets.UTF_8));
        final FileSystemStorage storage = new FileSystemStorage();
        storage.configure(ImmutableMap.of("root", root.toString()));
        storage.delete(TOPIC_PARTITION_SEGMENT_KEY);

        assertThat(keyPath).doesNotExist(); // segment key
        assertThat(keyPath.getParent()).doesNotExist(); // partition key
        assertThat(keyPath.getParent().getParent()).doesNotExist(); // partition key
        assertThat(root).exists();
    }

    @Test
    void testDeleteDoesNotRemoveParentDir() throws IOException, StorageBackendException {
        final String parent = "parent";
        final String key = "key";
        final Path parentPath = root.resolve(parent);
        Files.createDirectories(parentPath);
        Files.write(parentPath.resolve("another"), "test".getBytes(StandardCharsets.UTF_8));
        final Path keyPath = parentPath.resolve(key);
        Files.write(keyPath, "test".getBytes(StandardCharsets.UTF_8));
        final FileSystemStorage storage = new FileSystemStorage();
        storage.configure(ImmutableMap.of("root", root.toString()));
        storage.delete(new TestObjectKey(parent + "/" + key));

        assertThat(keyPath).doesNotExist();
        assertThat(parentPath).exists();
        assertThat(root).exists();
    }
}

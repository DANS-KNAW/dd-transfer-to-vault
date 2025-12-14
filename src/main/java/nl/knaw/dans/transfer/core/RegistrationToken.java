/*
 * Copyright (C) 2025 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
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
package nl.knaw.dans.transfer.core;

import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

@Value
@Slf4j
public class RegistrationToken {
    String nbn;
    URI location;

    public void save(Path path) {
        var properties = new Properties();
        properties.setProperty("nbn", nbn);
        properties.setProperty("location", location.toString());
        if (Files.exists(path)) {
            log.warn("Registration token already exists. Ignoring request...");
            return;
        }
        try (var outputStream = Files.newOutputStream(path)) {
            properties.store(outputStream, "Registration token");
        }
        catch (java.io.IOException e) {
            throw new RuntimeException("Failed to save registration token", e);
        }
    }

    public static RegistrationToken load(Path path) {
        var properties = new Properties();
        try (var inputStream = Files.newInputStream(path)) {
            properties.load(inputStream);
        }
        catch (java.io.IOException e) {
            throw new RuntimeException("Failed to load registration token", e);
        }
        var nbn = properties.getProperty("nbn");
        var location = URI.create(properties.getProperty("location"));
        return new RegistrationToken(nbn, location);
    }

}

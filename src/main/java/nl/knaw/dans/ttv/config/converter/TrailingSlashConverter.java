/*
 * Copyright (C) 2021 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
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
package nl.knaw.dans.ttv.config.converter;

import com.fasterxml.jackson.databind.util.StdConverter;

import java.net.URI;

/**
 * Adds a trailing slash to a URI if it is not already present.
 */
// TODO: move to dans-java-utils
public class TrailingSlashConverter extends StdConverter<URI, URI> {
    @Override
    public URI convert(URI uri) {
        uri = uri.normalize(); // Get rid of multiple trailing slashes
        return uri.normalize().getPath().endsWith("/") ? uri : URI.create(uri.toString() + "/");
    }
}

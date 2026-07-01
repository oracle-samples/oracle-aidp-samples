"use strict";
/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
Object.defineProperty(exports, "__esModule", { value: true });
const stream_1 = require("stream");
const buffer_1 = require("buffer");
const helper_1 = require("./helper");
const ChunkBuffer_1 = require("./chunks/ChunkBuffer");
const ChunkStream_1 = require("./chunks/ChunkStream");
const getReadableStreamData_1 = require("./chunks/getReadableStreamData");
const getReadableData_1 = require("./chunks/getReadableData");
function getChunk(data, partSize) {
    if (data instanceof buffer_1.Buffer) {
        return ChunkBuffer_1.ChunkBuffer(data, partSize);
    }
    else if (data instanceof stream_1.Readable) {
        return ChunkStream_1.ChunkStream(data, partSize, getReadableData_1.getReadableData);
    }
    else if (data instanceof String || typeof data === "string" || data instanceof Uint8Array) {
        // chunk Strings, Uint8Array.
        return ChunkBuffer_1.ChunkBuffer(buffer_1.Buffer.from(data), partSize);
    }
    if (typeof data.stream === "function") {
        // support for blob
        let stream = data.stream();
        if (stream.getReader) {
            return ChunkStream_1.ChunkStream(stream, partSize, getReadableStreamData_1.getReadableStreamData);
        }
        else {
            // Some fetch libraries have blob's .stream implemented as NodeJS's readable
            return ChunkStream_1.ChunkStream(stream, partSize, getReadableData_1.getReadableData);
        }
    }
    else if (helper_1.isReadableStream(data)) {
        // NodeJS run-time does not know what ReadableStream is, isReadableStream helps detect if stream is a ReadableStream
        return ChunkStream_1.ChunkStream(data, partSize, getReadableStreamData_1.getReadableStreamData);
    }
    else {
        throw new Error("Body Data is unsupported format, expected data to be one of: string | Uint8Array | Buffer | Readable | ReadableStream | Blob;.");
    }
}
exports.default = getChunk;
//# sourceMappingURL=chunker.js.map
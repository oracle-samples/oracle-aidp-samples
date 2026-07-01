"use strict";
/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
var __asyncValues = (this && this.__asyncValues) || function (o) {
    if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
    var m = o[Symbol.asyncIterator], i;
    return m ? m.call(o) : (o = typeof __values === "function" ? __values(o) : o[Symbol.iterator](), i = {}, verb("next"), verb("throw"), verb("return"), i[Symbol.asyncIterator] = function () { return this; }, i);
    function verb(n) { i[n] = o[n] && function (v) { return new Promise(function (resolve, reject) { v = o[n](v), settle(resolve, reject, v.done, v.value); }); }; }
    function settle(resolve, reject, d, v) { Promise.resolve(v).then(function(v) { resolve({ value: v, done: d }); }, reject); }
};
var __await = (this && this.__await) || function (v) { return this instanceof __await ? (this.v = v, this) : new __await(v); }
var __asyncGenerator = (this && this.__asyncGenerator) || function (thisArg, _arguments, generator) {
    if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
    var g = generator.apply(thisArg, _arguments || []), i, q = [];
    return i = {}, verb("next"), verb("throw"), verb("return"), i[Symbol.asyncIterator] = function () { return this; }, i;
    function verb(n) { if (g[n]) i[n] = function (v) { return new Promise(function (a, b) { q.push([n, v, a, b]) > 1 || resume(n, v); }); }; }
    function resume(n, v) { try { step(g[n](v)); } catch (e) { settle(q[0][3], e); } }
    function step(r) { r.value instanceof __await ? Promise.resolve(r.value.v).then(fulfill, reject) : settle(q[0][2], r); }
    function fulfill(value) { resume("next", value); }
    function reject(value) { resume("throw", value); }
    function settle(f, v) { if (f(v), q.shift(), q.length) resume(q[0][0], q[0][1]); }
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.ChunkStream = void 0;
const buffer_1 = require("buffer");
const crypto_1 = require("crypto");
const helper_1 = require("../helper");
function ChunkStream(data, partSize, getNextData) {
    return __asyncGenerator(this, arguments, function* ChunkStream_1() {
        var e_1, _a;
        const currentBuffer = { chunks: [], length: 0 };
        let content;
        let size;
        let md5Hash;
        try {
            for (var _b = __asyncValues(getNextData(data)), _c; _c = yield __await(_b.next()), !_c.done;) {
                const datum = _c.value;
                currentBuffer.chunks.push(datum);
                currentBuffer.length += datum.length;
                while (currentBuffer.length >= partSize) {
                    const dataChunk = currentBuffer.chunks.length > 1
                        ? buffer_1.Buffer.concat(currentBuffer.chunks)
                        : currentBuffer.chunks[0];
                    content = dataChunk.slice(0, partSize);
                    md5Hash = crypto_1.createHash("md5");
                    md5Hash.update(content);
                    size = helper_1.byteLength(content);
                    yield yield __await({
                        size: size,
                        data: content,
                        md5Hash: md5Hash.digest("base64")
                    });
                    // Reset buffer.
                    currentBuffer.chunks = [dataChunk.slice(partSize)];
                    currentBuffer.length = currentBuffer.chunks[0].length;
                }
            }
        }
        catch (e_1_1) { e_1 = { error: e_1_1 }; }
        finally {
            try {
                if (_c && !_c.done && (_a = _b.return)) yield __await(_a.call(_b));
            }
            finally { if (e_1) throw e_1.error; }
        }
        content = buffer_1.Buffer.concat(currentBuffer.chunks);
        size = helper_1.byteLength(content);
        md5Hash = crypto_1.createHash("md5");
        md5Hash.update(content);
        yield yield __await({
            size: size,
            data: content,
            md5Hash: md5Hash.digest("base64")
        });
    });
}
exports.ChunkStream = ChunkStream;
//# sourceMappingURL=ChunkStream.js.map
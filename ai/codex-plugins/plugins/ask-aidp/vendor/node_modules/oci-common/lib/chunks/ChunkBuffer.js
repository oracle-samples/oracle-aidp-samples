"use strict";
/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
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
exports.ChunkBuffer = void 0;
const crypto_1 = require("crypto");
const helper_1 = require("../helper");
function ChunkBuffer(data, partSize) {
    return __asyncGenerator(this, arguments, function* ChunkBuffer_1() {
        let partNumber = 1;
        let startByte = 0;
        let endByte = partSize;
        let content;
        let md5Hash;
        while (endByte < data.length) {
            content = data.slice(startByte, endByte);
            md5Hash = crypto_1.createHash("md5");
            md5Hash.update(content);
            yield yield __await({
                size: helper_1.byteLength(content),
                data: content,
                md5Hash: md5Hash.digest("base64")
            });
            partNumber += 1;
            startByte = endByte;
            endByte = startByte + partSize;
        }
        content = data.slice(startByte);
        md5Hash = crypto_1.createHash("md5");
        md5Hash.update(content);
        yield yield __await({
            size: helper_1.byteLength(content),
            data: content,
            md5Hash: md5Hash.digest("base64")
        });
    });
}
exports.ChunkBuffer = ChunkBuffer;
//# sourceMappingURL=ChunkBuffer.js.map
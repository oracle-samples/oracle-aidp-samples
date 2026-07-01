"use strict";
/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
/**
 * This is a helper class to generate in-memory temporary session keys.
 * <p>
 * The thread safety of this class is ensured
 */
const crypto_1 = require("crypto");
const key_pair_1 = __importDefault(require("./key-pair"));
class SessionKeySupplierImpl {
    constructor() {
        const { privateKey, publicKey } = crypto_1.generateKeyPairSync("rsa", {
            modulusLength: 2048,
            publicExponent: 65537,
            publicKeyEncoding: {
                type: "spki",
                format: "pem"
            },
            privateKeyEncoding: {
                type: "pkcs8",
                format: "pem"
            }
        });
        this.keyPair = new key_pair_1.default(publicKey, privateKey);
    }
    getKeyPair() {
        return this.keyPair;
    }
    refreshKeys() {
        const { privateKey, publicKey } = crypto_1.generateKeyPairSync("rsa", {
            modulusLength: 2048,
            publicExponent: 65537,
            publicKeyEncoding: {
                type: "spki",
                format: "pem"
            },
            privateKeyEncoding: {
                type: "pkcs8",
                format: "pem"
            }
        });
        this.keyPair = new key_pair_1.default(publicKey, privateKey);
    }
}
exports.default = SessionKeySupplierImpl;
//# sourceMappingURL=session-key-supplier.js.map
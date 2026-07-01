"use strict";
/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
Object.defineProperty(exports, "__esModule", { value: true });
/**
 * This class is a simple holder for a key pair (a public key and a
 * private key).
 */
class KeyPair {
    constructor(publicKey, privateKey) {
        this.publicKey = publicKey;
        this.privateKey = privateKey;
    }
    getPublic() {
        return this.publicKey;
    }
    getPrivate() {
        return this.privateKey;
    }
}
exports.default = KeyPair;
//# sourceMappingURL=key-pair.js.map
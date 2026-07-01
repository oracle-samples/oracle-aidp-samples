"use strict";
/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const jsonwebtoken_1 = __importDefault(require("jsonwebtoken"));
/**
 * Helper class to store security token and sessionKeySupplier
 * contain methods to check if token is expired and needs to be refreshed
 */
class SecurityTokenAdapter {
    constructor(securityToken, sessionKeySupplier) {
        this.securityToken = securityToken;
        this.sessionKeySupplier = sessionKeySupplier;
        this.jwt = null;
        this.jwt = this.securityToken ? this.parse(this.securityToken) : null;
    }
    /**
     * Getter for securityToken
     * @returns securityToken
     */
    getSecurityToken() {
        return this.securityToken;
    }
    /**
     * Helper method to verify token's secret matches publicKey string
     * then parse token string into an complete object (header, payload, signature)
     * @param token: string
     * @returns payload: object
     */
    parse(token) {
        try {
            const payload = jsonwebtoken_1.default.decode(token, { complete: true });
            return payload;
        }
        catch (e) {
            throw Error(`Failed to decode token, error: ${e}`);
        }
    }
    /**
     * Checks to see current token exists, and if so, check expiration time
     * @return true if valid
     */
    isValid() {
        const secondsSinceEpoch = Math.round(Date.now() / 1000);
        if (this.jwt == null) {
            return false;
        }
        else if (this.jwt.payload && this.jwt.payload.exp > secondsSinceEpoch) {
            return true;
        }
        return false;
    }
    /**
     * Return a claim from the token given a key if it exist, else return null
     * @param key: string
     * @return value of the claim
     */
    getStringClaim(key) {
        if (this.jwt == null) {
            return null;
        }
        return this.jwt.payload[key] ? this.jwt.payload[key] : null;
    }
}
exports.default = SecurityTokenAdapter;
//# sourceMappingURL=security-token-adapter.js.map
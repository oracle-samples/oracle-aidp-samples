"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
const fs_1 = require("fs");
const security_token_adapter_1 = __importDefault(require("./security-token-adapter"));
/**
 * This class gets a security token from file.
 */
class FileBasedResourcePrincipalFederationClient {
    constructor(sessionKeySupplier, resourcePrincipalSessionTokenPath) {
        this.sessionKeySupplier = sessionKeySupplier;
        this.resourcePrincipalSessionTokenPath = resourcePrincipalSessionTokenPath;
        this.sessionKeySupplier = sessionKeySupplier;
        this.securityTokenAdapter = new security_token_adapter_1.default("", sessionKeySupplier);
        this.resourcePrincipalSessionTokenPath = resourcePrincipalSessionTokenPath;
    }
    /**
     * Gets a security token. If there is already a valid token cached, it will be returned. Else this will make a call
     * to the auth service to get a new token, using the provided suppliers.
     *
     * This method is thread-safe.
     * @return the security token
     * @throws OciError If there is any issue with getting a token from the auth server
     */
    getSecurityToken() {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.securityTokenAdapter.isValid()) {
                return this.securityTokenAdapter.getSecurityToken();
            }
            return yield this.refreshAndGetSecurityTokenInner(true);
        });
    }
    /**
     * Return a claim embedded in the security token
     * @param key the name of the claim
     * @return the value of the claim or null if unable to find
     */
    getStringClaim(key) {
        return __awaiter(this, void 0, void 0, function* () {
            yield this.refreshAndGetSecurityToken();
            return this.securityTokenAdapter.getStringClaim(key);
        });
    }
    refreshAndGetSecurityToken() {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this.refreshAndGetSecurityTokenInner(false);
        });
    }
    refreshAndGetSecurityTokenInner(doFinalTokenValidityCheck) {
        return __awaiter(this, void 0, void 0, function* () {
            // Check again to see if the JWT is still invalid, unless we want to skip that check
            if (!doFinalTokenValidityCheck || !this.securityTokenAdapter.isValid()) {
                this.sessionKeySupplier.refreshKeys();
                this.securityTokenAdapter = this.getSecurityTokenFromFile();
                return this.securityTokenAdapter.getSecurityToken();
            }
            return this.securityTokenAdapter.getSecurityToken();
        });
    }
    /**
     * Gets a security token from file
     * @return the security token, which is basically a JWT token string
     */
    getSecurityTokenFromFile() {
        const keyPair = this.sessionKeySupplier.getKeyPair();
        if (!keyPair) {
            throw Error("Keypair for session was not provided");
        }
        let securityToken = "";
        try {
            securityToken = fs_1.readFileSync(this.resourcePrincipalSessionTokenPath, "utf8");
        }
        catch (e) {
            throw Error(`Failed to read token due to error: ${e}`);
        }
        return new security_token_adapter_1.default(securityToken, this.sessionKeySupplier);
    }
}
exports.default = FileBasedResourcePrincipalFederationClient;
//# sourceMappingURL=file-based-resource-principal-federation-client.js.map
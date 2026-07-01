"use strict";
/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
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
const security_token_adapter_1 = __importDefault(require("./security-token-adapter"));
/**
 * This class returns a security token, supplied as a fixed value.
 */
class FixedContentResourcePrincipalFederationClient {
    /**
     * Constructor of FixedContentResourcePrincipalFederationClient.
     *
     * @param resourcePrincipalSessionToken the constant value for the RPST
     * @param sessionKeySupplier the associated SessionKeySupplier
     */
    constructor(resourcePrincipalSessionToken, sessionKeySupplier) {
        this.securityTokenAdapter = new security_token_adapter_1.default(resourcePrincipalSessionToken, sessionKeySupplier);
    }
    getSecurityToken() {
        return __awaiter(this, void 0, void 0, function* () {
            return this.securityTokenAdapter.getSecurityToken();
        });
    }
    refreshAndGetSecurityToken() {
        return __awaiter(this, void 0, void 0, function* () {
            return this.securityTokenAdapter.getSecurityToken();
        });
    }
    getStringClaim(key) {
        return __awaiter(this, void 0, void 0, function* () {
            return this.securityTokenAdapter.getStringClaim(key);
        });
    }
}
exports.default = FixedContentResourcePrincipalFederationClient;
//# sourceMappingURL=fixed-content-resource-principal-federation-client.js.map
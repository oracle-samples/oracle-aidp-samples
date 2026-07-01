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
const X509_federation_client_1 = __importDefault(require("./X509-federation-client"));
/**
 * Base class for authentication details providers that make remote requests.
 */
class AbstractRequestingAuthenticationDetailsProvider {
    constructor(federationClient, sessionKeySupplier) {
        this.federationClient = federationClient;
        this.sessionKeySupplier = sessionKeySupplier;
    }
    getKeyId() {
        return __awaiter(this, void 0, void 0, function* () {
            return "ST$" + (yield this.federationClient.getSecurityToken());
        });
    }
    getPrivateKey() {
        return this.sessionKeySupplier.getKeyPair().getPrivate();
    }
    getPassphrase() {
        return null;
    }
    closeProvider() {
        if (this.federationClient instanceof X509_federation_client_1.default) {
            this.federationClient.close();
        }
    }
}
exports.default = AbstractRequestingAuthenticationDetailsProvider;
//# sourceMappingURL=abstract-requesting-authentication-detail-provider.js.map
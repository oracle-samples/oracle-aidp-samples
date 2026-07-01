/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
/**
 * The purpose of this file is to help taken in the delegated task of assigning a true auth provider to the
 * authentication provider.
 */
import auth = require("../auth");
export declare function delegateAuthProvider(authenticationDetailsProvider: auth.AuthenticationDetailsProvider): Promise<auth.AuthenticationDetailsProvider | null>;

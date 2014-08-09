/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springdata.cql.core;

import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.datastax.driver.core.policies.RetryPolicy;

/**
 * RetryPolicy predefined singletons
 * 
 * @author Alex Shvid
 * 
 */

public enum RetryPolicyInstance {

	DEFAULT(DefaultRetryPolicy.INSTANCE), DOWNGRADING_CONSISTENCY(DowngradingConsistencyRetryPolicy.INSTANCE), FALLTHROUGH(
			FallthroughRetryPolicy.INSTANCE);

	private final com.datastax.driver.core.policies.RetryPolicy instance;

	private RetryPolicyInstance(RetryPolicy instance) {
		this.instance = instance;
	}

	public RetryPolicy getInstance() {
		return instance;
	}

}

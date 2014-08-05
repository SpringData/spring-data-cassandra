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
package org.springdata.cql.config.java;

import org.springdata.cql.config.CompressionType;
import org.springdata.cql.config.CqlClusterFactoryBean;
import org.springdata.cql.config.PoolingOptions;
import org.springdata.cql.config.SocketOptions;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RetryPolicy;

/**
 * 
 * @author Alex Shvid
 * 
 */

@Configuration
public class AbstractClusterConfiguration {

	/**
	 * Return the {@link Cluster} instance to connect to.
	 * 
	 * @return Cluster object
	 */
	@Bean
	public CqlClusterFactoryBean cluster() {

		CqlClusterFactoryBean bean = new CqlClusterFactoryBean();
		bean.setAuthProvider(authProvider());
		bean.setCompressionType(compressionType());
		bean.setContactPoints(contactPoints());
		bean.setPort(port());
		bean.setMetricsEnabled(metricsEnabled());
		bean.setLoadBalancingPolicy(loadBalancingPolicy());
		bean.setReconnectionPolicy(reconnectionPolicy());
		bean.setLocalPoolingOptions(localPoolingOptions());
		bean.setRemotePoolingOptions(remotePoolingOptions());
		bean.setRetryPolicy(retryPolicy());
		bean.setSocketOptions(socketOptions());

		return bean;
	}

	protected AuthProvider authProvider() {
		return null;
	}

	protected CompressionType compressionType() {
		return null;
	}

	protected String contactPoints() {
		return CqlClusterFactoryBean.DEFAULT_CONTACT_POINTS;
	}

	protected int port() {
		return CqlClusterFactoryBean.DEFAULT_PORT;
	}

	protected boolean metricsEnabled() {
		return CqlClusterFactoryBean.DEFAULT_METRICS_ENABLED;
	}

	protected LoadBalancingPolicy loadBalancingPolicy() {
		return null;
	}

	protected ReconnectionPolicy reconnectionPolicy() {
		return null;
	}

	protected PoolingOptions localPoolingOptions() {
		return null;
	}

	protected PoolingOptions remotePoolingOptions() {
		return null;
	}

	protected RetryPolicy retryPolicy() {
		return null;
	}

	protected SocketOptions socketOptions() {
		return null;
	}

}

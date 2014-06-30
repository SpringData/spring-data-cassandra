/*
 * Copyright 2013 the original author or authors.
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
package org.springdata.cassandra.repository;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springdata.cassandra.cql.core.ConsistencyLevel;
import org.springdata.cassandra.cql.core.RetryPolicy;

/**
 * Annotation to declare query options for methods.
 * 
 * @author Alex Shvid
 */

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Documented
public @interface QueryOptions {

	ConsistencyLevel consistencyLevel() default ConsistencyLevel.QUORUM;

	RetryPolicy retryPolicy() default RetryPolicy.DEFAULT;

	int timeToLiveSeconds() default -1;

	long timestampMilliseconds() default -1L;

}

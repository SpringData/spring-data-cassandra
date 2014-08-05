/*
 * Copyright 2013-2014 the original author or authors.
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
package org.springdata.cql.config.xml;

import java.util.List;

import org.springdata.cql.config.KeyspaceAttributes;
import org.springdata.cql.core.SessionFactoryBean;
import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSimpleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;

/**
 * Parser for Cassandra Cql Session definitions.
 * 
 * @author Alex Shvid
 */

public class CassandraCqlSessionParser extends AbstractSimpleBeanDefinitionParser {

	@Override
	protected Class<?> getBeanClass(Element element) {
		return SessionFactoryBean.class;
	}

	@Override
	protected String resolveId(Element element, AbstractBeanDefinition definition, ParserContext parserContext)
			throws BeanDefinitionStoreException {

		String id = super.resolveId(element, definition, parserContext);
		return StringUtils.hasText(id) ? id : ConfigCqlConstants.CASSANDRA_SESSION;
	}

	@Override
	protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {

		String keyspace = element.getAttribute("keyspace");
		if (StringUtils.hasText(keyspace)) {
			builder.addPropertyValue("keyspace", keyspace);
		}

		String clusterRef = element.getAttribute("cluster-ref");
		if (!StringUtils.hasText(clusterRef)) {
			clusterRef = ConfigCqlConstants.CASSANDRA_CLUSTER;
		}
		builder.addPropertyReference("cluster", clusterRef);

		postProcess(builder, element);
	}

	@Override
	protected void postProcess(BeanDefinitionBuilder builder, Element element) {
		List<Element> subElements = DomUtils.getChildElements(element);

		// parse nested elements
		for (Element subElement : subElements) {
			String name = subElement.getLocalName();

			if ("keyspace-attributes".equals(name)) {
				builder.addPropertyValue("keyspaceAttributes", parseKeyspaceAttributes(subElement));
			}
		}

	}

	private BeanDefinition parseKeyspaceAttributes(Element element) {
		BeanDefinitionBuilder defBuilder = BeanDefinitionBuilder.genericBeanDefinition(KeyspaceAttributes.class);
		ParsingUtils.setPropertyValue(defBuilder, element, "action", "actionStr");
		ParsingUtils.setPropertyValue(defBuilder, element, "replication-strategy", "replicationStrategy");
		ParsingUtils.setPropertyValue(defBuilder, element, "replication-factor", "replicationFactor");
		ParsingUtils.setPropertyValue(defBuilder, element, "durable-writes", "durableWrites");
		return defBuilder.getBeanDefinition();
	}

}

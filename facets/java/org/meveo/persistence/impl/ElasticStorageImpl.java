/**
 * 
 */
package org.meveo.persistence.impl;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.enterprise.context.RequestScoped;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.meveo.admin.exception.BusinessException;
import org.meveo.admin.util.pagination.PaginationConfiguration;
import org.meveo.api.exception.EntityDoesNotExistsException;
import org.meveo.model.crm.CustomFieldTemplate;
import org.meveo.model.customEntities.CustomEntityInstance;
import org.meveo.model.customEntities.CustomEntityTemplate;
import org.meveo.model.customEntities.CustomModelObject;
import org.meveo.model.customEntities.CustomRelationshipTemplate;
import org.meveo.model.persistence.DBStorageType;
import org.meveo.model.storage.Repository;
import org.meveo.persistence.PersistenceActionResult;
import org.meveo.persistence.StorageImpl;
import org.meveo.persistence.StorageQuery;
import org.meveo.service.crm.impl.CustomFieldTemplateService;
import org.meveo.service.script.Script;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.mapping.LongNumberProperty;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch._types.mapping.TextProperty;
import co.elastic.clients.elasticsearch._types.mapping.WildcardProperty;
import co.elastic.clients.elasticsearch.core.DeleteRequest;
import co.elastic.clients.elasticsearch.core.ExistsRequest;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.UpdateRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import co.elastic.clients.elasticsearch.indices.PutMappingRequest;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;

@RequestScoped
public class ElasticStorageImpl extends Script implements StorageImpl {

	private CustomFieldTemplateService cftService = getCDIBean(CustomFieldTemplateService.class);

	private Map<String, ElasticsearchClient> clients = new ConcurrentHashMap<String, ElasticsearchClient>();

	private static Logger LOG = LoggerFactory.getLogger(ElasticStorageImpl.class);

	private static DBStorageType storageType() {
		DBStorageType dbStorageType = new DBStorageType();
		dbStorageType.setCode("ELASTIC");
		return dbStorageType;
	}

	@Override
	public boolean exists(Repository repository, CustomEntityTemplate cet, String uuid) {
		ElasticsearchClient client = beginTransaction(repository, 0);

		ExistsRequest request = new ExistsRequest.Builder()
			.index(cet.getCode())
			.id(uuid)
			.build();

		try {
			return client.exists(request).value();
		} catch (ElasticsearchException | IOException e) {
			LOG.error("Failed to execute reqeust", e);
			return false;
		}
	}

	private SearchRequest buildSearchRequest(StorageQuery query, Map<String, CustomFieldTemplate> fields) {
		return new SearchRequest.Builder()
			.index(query.getCet().getCode())
			.query(builder -> {
				builder.bool(boolQuery -> {
					boolQuery.filter(filterQuery -> {
						query.getFilters().forEach((field, value) -> {
							var cft = fields.get(field);
							switch (cft.getFieldType()) {
								case TEXT_AREA:
								case LONG_TEXT:
								case STRING:
									filterQuery.match(matchBuilder -> matchBuilder.field(field)
										.query(q -> q.stringValue((String) value)));
								default:
									break;
							}
						});
						return filterQuery;
					});
					return boolQuery;
				});
				return builder;
			}).build();
	}

	@Override
	public String findEntityIdByValues(Repository repository, CustomEntityInstance cei) {
		StorageQuery query = new StorageQuery();
		query.setCet(cei.getCet());
		query.setFilters(cei.getCfValuesAsValues(storageType(), cei.getFieldTemplates().values(), true));

		SearchRequest request = buildSearchRequest(query, cei.getFieldTemplates());

		ElasticsearchClient client = beginTransaction(repository, 0);
		try {
			var response = client.search(request, Map.class);
			if (!response.hits().hits().isEmpty()) {
				return response.hits().hits().get(0).id();
			} else {
				return null;
			}
		} catch (ElasticsearchException | IOException e) {
			LOG.error("Failed to retrieve id", e);
		}

		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<String, Object> findById(Repository repository, CustomEntityTemplate cet, String uuid,
			Map<String, CustomFieldTemplate> cfts, Collection<String> fetchFields, boolean withEntityReferences) {

		SearchRequest request = new SearchRequest.Builder()
				.index(cet.getCode())
				.query(builder -> builder.term(term -> term.field("_id").value(FieldValue.of(uuid))))
				.build();

		ElasticsearchClient client = beginTransaction(repository, 0);

		try {
			var response = client.search(request, Map.class);
			if (!response.hits().hits().isEmpty()) {
				return response.hits().hits().get(0).source();
			} else {
				return null;
			}
		} catch (ElasticsearchException | IOException e) {
			LOG.error("Failed to retrieve value", e);
			return null;
		}
	}

	@Override
	public List<Map<String, Object>> find(StorageQuery query) throws EntityDoesNotExistsException {
		var fieldsTemplates = cftService.findByAppliesTo(query.getCet().getAppliesTo());
		SearchRequest request = buildSearchRequest(query, fieldsTemplates);

		ElasticsearchClient client = beginTransaction(query.getRepository(), 0);
		try {
			var response = client.search(request, Map.class);
			if (!response.hits().hits().isEmpty()) {
				return response.hits().hits()
					.stream()
					.map(hit -> hit.fields()
						.entrySet()
						.stream()
						.collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue().to(Object.class)))
					).collect(Collectors.toList());

			} else {
				return null;
			}
		} catch (ElasticsearchException | IOException e) {
			LOG.error("Failed to retrieve data", e);
		}

		return null;
	}

	@Override
	public PersistenceActionResult createOrUpdate(Repository repository, CustomEntityInstance cei,
			Map<String, CustomFieldTemplate> customFieldTemplates, String foundUuid) throws BusinessException {
		
		if (this.exists(repository, cei.getCet(), cei.getUuid())) {
			this.update(repository, cei);
			return new PersistenceActionResult(cei.getUuid());
		} else {
			var request = new IndexRequest.Builder<>()
				.index(cei.getCetCode())
				.document(cei.getValues(storageType()))
				.build();

			ElasticsearchClient client = beginTransaction(repository, 0);

			try {
				var response = client.index(request);
				return new PersistenceActionResult(response.id());
			} catch (ElasticsearchException | IOException e) {
				LOG.error("Failed to create data", e);
			}
		}

		return null;
	}

	@Override
	public PersistenceActionResult addCRTByUuids(Repository repository, CustomRelationshipTemplate crt,
			Map<String, Object> relationValues, String sourceUuid, String targetUuid) throws BusinessException {
		return null;
	}

	@Override
	public void update(Repository repository, CustomEntityInstance cei) throws BusinessException {
		var request = new UpdateRequest.Builder<>()
				.index(cei.getCetCode())
				.doc(cei.getValues(storageType()))
				.id(cei.getUuid())
				.build();

		ElasticsearchClient client = beginTransaction(repository, 0);

		try {
			client.update(request, Object.class);
		} catch (ElasticsearchException | IOException e) {
			LOG.error("Failed to update data", e);
		}
	}

	@Override
	public void setBinaries(Repository repository, CustomEntityTemplate cet, CustomFieldTemplate cft, String uuid,
			List<File> binaries) throws BusinessException {

	}

	@Override
	public void remove(Repository repository, CustomEntityTemplate cet, String uuid) throws BusinessException {
		var request = new DeleteRequest.Builder()
			.index(cet.getCode())
			.id(uuid)
			.build();

		ElasticsearchClient client = beginTransaction(repository, 0);
		try {
			client.delete(request);
		} catch (ElasticsearchException | IOException e) {
			LOG.error("Failed to delete data", e);
		}
	}

	@Override
	public Integer count(Repository repository, CustomEntityTemplate cet,
			PaginationConfiguration paginationConfiguration) {
		
		Map<String, Object> filters = new HashMap<>(paginationConfiguration.getFilters());
		Map<String, CustomFieldTemplate> cfts = cftService.findByAppliesTo(cet.getAppliesTo());

		cfts.values().forEach(cft -> {
			if (!cft.getStorages().contains(storageType())) {
				filters.remove(cft.getCode());
			}
		});
		StorageQuery query = new StorageQuery();
		query.setCet(cet);
		query.setFilters(filters);

		SearchRequest request = buildSearchRequest(query, cfts);

		ElasticsearchClient client = beginTransaction(query.getRepository(), 0);

		//TODO: Use CountResquest
		try {
			var response = client.search(request, Map.class);
			return Long.valueOf(response.hits().total().value()).intValue();
		} catch (ElasticsearchException | IOException e) {
			LOG.error("Failed to retrieve data", e);
		}

		return null;
	}

	@Override
	public void cetCreated(CustomEntityTemplate cet) {
		for (var repository : cet.getRepositories()) {
			ElasticsearchClient client = beginTransaction(repository, 0);

			CreateIndexRequest request = new CreateIndexRequest.Builder()
				.index(cet.getCode())
				.build();

			try {
				client.indices().create(request);
			} catch (IOException e) {
				LOG.error("Failed to create index for {}", cet.getCode());
			}
		}
	}

	@Override
	public void removeCet(CustomEntityTemplate cet) {
		for (var repository : cet.getRepositories()) {
			ElasticsearchClient client = beginTransaction(repository, 0);
			DeleteIndexRequest request = new DeleteIndexRequest.Builder()
				.index(cet.getCode())
				.build();

			try {
				client.indices().delete(request);
			} catch (IOException e) {
				LOG.error("Failed to delete index for {}", cet.getCode());
			}
		}
	}

	@Override
	public void crtCreated(CustomRelationshipTemplate crt) throws BusinessException {
		

	}

	@Override
	public void cftCreated(CustomModelObject template, CustomFieldTemplate cft) {
		for (var repository : template.getRepositories()) {
			ElasticsearchClient client = beginTransaction(repository, 0);

			PutMappingRequest request = new PutMappingRequest.Builder()
				.properties(Map.of(cft.getCode(), getPropertyFromCft(cft)))
				.type(template.getCode())
				.build();

			try {
				client.indices().putMapping(request);
			} catch (IOException e) {
				LOG.error("Failed to create mapping for {}", template.getCode());
			}
		}

	}

	@Override
	public void cetUpdated(CustomEntityTemplate oldCet, CustomEntityTemplate cet) {
		

	}

	@Override
	public void crtUpdated(CustomRelationshipTemplate cet) throws BusinessException {
		

	}

	@Override
	public void cftUpdated(CustomModelObject template, CustomFieldTemplate oldCft, CustomFieldTemplate cft) {
		
	}

	@Override
	public void removeCft(CustomModelObject template, CustomFieldTemplate cft) {
		

	}

	@Override
	public void removeCrt(CustomRelationshipTemplate crt) {
		

	}

	@Override
	public void init() {

	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T beginTransaction(Repository repository, int stackedCalls) {
		return (T) clients.computeIfAbsent(repository.getCode(), code -> {
			var cfValues = repository.getCfValues();
			String elasticHost = cfValues.getCfValue("elasticHost").getStringValue();
			int elasticPort = cfValues.getCfValue("elasticPort").getLongValue().intValue();
			String elasticUsername = cfValues.getCfValue("elasticUsername").getStringValue();
			String elasticPassword = cfValues.getCfValue("elasticPassword").getStringValue(); // TODO: Decrypt password

			final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

			credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(elasticUsername, elasticPassword));

			RestClientBuilder builder = RestClient.builder(
					new HttpHost(elasticHost, elasticPort))
					.setHttpClientConfigCallback(new HttpClientConfigCallback() {
						@Override
						public HttpAsyncClientBuilder customizeHttpClient(
								HttpAsyncClientBuilder httpClientBuilder) {
							return httpClientBuilder
									.setDefaultCredentialsProvider(credentialsProvider);
						}
					});

			RestClient restClient = builder.build();

			ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());

			ElasticsearchClient client = new ElasticsearchClient(transport);

			return client;
		});
	}

	@Override
	public void commitTransaction(Repository repository) {
		// NOOP
	}

	@Override
	public void rollbackTransaction(int stackedCalls) {
		// NOOP
	}

	@Override
	public void destroy() {
		clients.values().forEach(client -> {
			try {
				client._transport().close();
			} catch (IOException e) {
				LOG.error("Failed to close elastic client", e);
			}
		});
	}

	private static Property getPropertyFromCft(CustomFieldTemplate cft) {
		Property.Builder propertyBuilder = new Property.Builder();

		switch (cft.getFieldType()) {
			case LONG:
				LongNumberProperty longNumberProperty = new LongNumberProperty.Builder()
					.build();
				propertyBuilder.long_(longNumberProperty);
				break;

			case LONG_TEXT:
			case TEXT_AREA:
				TextProperty textProperty = new TextProperty.Builder().build();
				propertyBuilder.text(textProperty);
				break;
			case STRING:
				propertyBuilder.wildcard(new WildcardProperty.Builder().build());
				break;
			default:
				break;
		}

		return propertyBuilder.build();
	}

}

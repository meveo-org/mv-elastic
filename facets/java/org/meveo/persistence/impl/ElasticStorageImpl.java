package org.meveo.persistence.impl;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.enterprise.context.RequestScoped;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.commons.httpclient.HttpHost;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.auth.CredentialsProvider;
import org.apache.http.HttpEntity;
import org.jboss.resteasy.client.jaxrs.BasicAuthentication;
import org.meveo.admin.exception.BusinessException;
import org.meveo.admin.util.pagination.PaginationConfiguration;
import org.meveo.api.exception.EntityDoesNotExistsException;
import org.meveo.elastic.ElasticRestClient;
import org.meveo.model.crm.CustomFieldTemplate;
import org.meveo.model.customEntities.CustomEntityInstance;
import org.meveo.model.customEntities.CustomEntityTemplate;
import org.meveo.model.customEntities.CustomModelObject;
import org.meveo.model.customEntities.CustomRelationshipTemplate;
import org.meveo.model.persistence.DBStorageType;
import org.meveo.model.persistence.JacksonUtil;
import org.meveo.model.storage.Repository;
import org.meveo.persistence.PersistenceActionResult;
import org.meveo.persistence.StorageImpl;
import org.meveo.persistence.StorageQuery;
import org.meveo.service.crm.impl.CustomFieldInstanceService;
import org.meveo.service.crm.impl.CustomFieldTemplateService;
import org.meveo.service.script.Script;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RequestScoped
public class ElasticStorageImpl extends Script implements StorageImpl {

	private CustomFieldTemplateService cftService = getCDIBean(CustomFieldTemplateService.class);
	
	private CustomFieldInstanceService cfiService = getCDIBean(CustomFieldInstanceService.class);

	private Map<String, ElasticRestClient> clients = new ConcurrentHashMap<>();

	private static Logger LOG = LoggerFactory.getLogger(ElasticStorageImpl.class);

	private static DBStorageType storageType() {
		DBStorageType dbStorageType = new DBStorageType();
		dbStorageType.setCode("ELASTIC");
		return dbStorageType;
	}

	@Override
	public boolean exists(Repository repository, CustomEntityTemplate cet, String uuid) {
		ElasticRestClient client = beginTransaction(repository, 0);

		int status = client.head("/%s/%s", cet.getCode().toLowerCase(), uuid);

		return status == 200;
	}

	private String buildSearchRequest(StorageQuery query, Map<String, CustomFieldTemplate> fields) {
		var json = JacksonUtil.OBJECT_MAPPER.createObjectNode();

		var queries = json.putObject("query")
			.putObject("bool")
			.putArray("must");

		query.getFilters().forEach((filterKey, filterValue) -> {
			if (!filterKey.equals("uuid")) {
				queries.addObject()
					.putObject("match")
					.put(filterKey.toLowerCase(), String.valueOf(filterValue));
			}
		});
		
		return json.toString();
	}

	@Override
	public String findEntityIdByValues(Repository repository, CustomEntityInstance cei) {
		return null;
		// StorageQuery query = new StorageQuery();
		// query.setCet(cei.getCet());
		// query.setFilters(cei.getCfValuesAsValues(storageType(), cei.getFieldTemplates().values(), true));

		// SearchRequest request = buildSearchRequest(query, cei.getFieldTemplates());

		// ElasticsearchClient client = beginTransaction(repository, 0);
		// try {
		// 	var response = client.search(request, Map.class);
		// 	if (!response.hits().hits().isEmpty()) {
		// 		return response.hits().hits().get(0).id();
		// 	} else {
		// 		return null;
		// 	}
		// } catch (IOException e) {
		// 	LOG.error("Failed to retrieve id", e);
		// }

		// return null;
	}

	private Map<String, Object> mapHitToCfts(JsonNode hit, Collection<CustomFieldTemplate> cfts) {
		Map<String, Object> resultData = new HashMap<>();
		resultData.put("uuid", hit.get("_id").asText());
		var _source = hit.get("_source");

		cfts.forEach((cft) -> {
			var fieldValue = _source.get(cft.getCode().toLowerCase());
			if (fieldValue != null) {
				Object convertedValue; 
				switch (cft.getFieldType()) {
					case DOUBLE:
						convertedValue = fieldValue.asDouble();
						break;
					case LONG: 
						convertedValue = fieldValue.asLong();
						break;
					case BOOLEAN:
						convertedValue = fieldValue.asBoolean();
					default:
						convertedValue = fieldValue.asText();
				}
				resultData.put(cft.getCode(), convertedValue);
			}
		});

		return resultData;
	}

	@Override
	public Map<String, Object> findById(Repository repository, CustomEntityTemplate cet, String uuid,
			Map<String, CustomFieldTemplate> cfts, Collection<String> fetchFields, boolean withEntityReferences) {
			
		ElasticRestClient client = beginTransaction(repository, 0);
		var request = client.get("/%s/_doc/%s", cet.getCode().toLowerCase(), uuid);
		return client.execute(request, response -> {
			try {
				var responseJson =  JacksonUtil.OBJECT_MAPPER.readTree(response.getEntity().getContent());
				LOG.info("Find by id {} = {}", uuid, responseJson);
				return mapHitToCfts(responseJson, cfts.values());

			} catch (UnsupportedOperationException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			return null;
		});
	}

	@Override
	public List<Map<String, Object>> find(StorageQuery query) throws EntityDoesNotExistsException {
		ElasticRestClient client = beginTransaction(query.getRepository(), 0);
		var fieldsTemplates = cftService.findByAppliesTo(query.getCet().getAppliesTo());

		var get = client.get("/%s/_search", query.getCet().getCode().toLowerCase());
		client.setBody(get, buildSearchRequest(query, fieldsTemplates));

		return (List<Map<String, Object>>) client.execute(get, response -> {
			var mapper = JacksonUtil.OBJECT_MAPPER;
			try {
				var responseJson =  mapper.readTree(response.getEntity().getContent());
				LOG.info("Search result = " + responseJson);
				
				var hits = responseJson.get("hits").get("hits");

				return StreamSupport.stream(hits.spliterator(), false)
					.map(node -> mapHitToCfts(node, fieldsTemplates.values()))
					.collect(Collectors.toList());

			} catch (UnsupportedOperationException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
		});
	}

	@Override
	public PersistenceActionResult createOrUpdate(Repository repository, CustomEntityInstance cei,
			Map<String, CustomFieldTemplate> customFieldTemplates, String foundUuid) throws BusinessException {
		
		if (this.exists(repository, cei.getCet(), cei.getUuid())) {
			this.update(repository, cei);
			return new PersistenceActionResult(cei.getUuid());
		} else {

			ElasticRestClient client = beginTransaction(repository, 0);

			var put = client.put("/%s/_create/%s", cei.getCetCode().toLowerCase(), cei.getUuid());
			client.setBody(put, JacksonUtil.toString(cei.getValues(storageType())));
			return client.execute(put, response -> {
				try {
					var json = JacksonUtil.OBJECT_MAPPER.readTree(response.getEntity().getContent());
					if (json.get("result").asText().equals("created")) {
						return new PersistenceActionResult(json.get("_id").asText());
					} else {
						//TODO
						return null;
					}
				} catch (UnsupportedOperationException | IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return null;
				}
			});
		}
	}

	@Override
	public PersistenceActionResult addCRTByUuids(Repository repository, CustomRelationshipTemplate crt,
			Map<String, Object> relationValues, String sourceUuid, String targetUuid) throws BusinessException {
		return null;
	}

	@Override
	public void update(Repository repository, CustomEntityInstance cei) throws BusinessException {
		// var request = new UpdateRequest.Builder<>()
		// 		.index(cei.getCetCode())
		// 		.doc(cei.getValues(storageType()))
		// 		.id(cei.getUuid())
		// 		.build();

		// ElasticsearchClient client = beginTransaction(repository, 0);

		// try {
		// 	client.update(request, Object.class);
		// } catch (IOException e) {
		// 	LOG.error("Failed to update data", e);
		// }
	}

	@Override
	public void setBinaries(Repository repository, CustomEntityTemplate cet, CustomFieldTemplate cft, String uuid,
			List<File> binaries) throws BusinessException {

	}

	@Override
	public void remove(Repository repository, CustomEntityTemplate cet, String uuid) throws BusinessException {
		// var request = new DeleteRequest.Builder()
		// 	.index(cet.getCode())
		// 	.id(uuid)
		// 	.build();

		// ElasticsearchClient client = beginTransaction(repository, 0);
		// try {
		// 	client.delete(request);
		// } catch (IOException e) {
		// 	LOG.error("Failed to delete data", e);
		// }
	}

	@Override
	public Integer count(Repository repository, CustomEntityTemplate cet,
			PaginationConfiguration paginationConfiguration) {
		
				return 10;
		// Map<String, Object> filters = new HashMap<>(paginationConfiguration.getFilters());
		// Map<String, CustomFieldTemplate> cfts = cftService.findByAppliesTo(cet.getAppliesTo());

		// cfts.values().forEach(cft -> {
		// 	if (!cft.getStorages().contains(storageType())) {
		// 		filters.remove(cft.getCode());
		// 	}
		// });
		// StorageQuery query = new StorageQuery();
		// query.setCet(cet);
		// query.setFilters(filters);

		// SearchRequest request = buildSearchRequest(query, cfts);

		// ElasticsearchClient client = beginTransaction(query.getRepository(), 0);

		// //TODO: Use CountResquest
		// try {
		// 	var response = client.search(request, Map.class);
		// 	return Long.valueOf(response.hits().total().value()).intValue();
		// } catch (IOException e) {
		// 	LOG.error("Failed to retrieve data", e);
		// }

		// return null;
	}

	@Override
	public void cetCreated(CustomEntityTemplate cet) {
		for (var repository : cet.getRepositories()) {
			ElasticRestClient client = beginTransaction(repository, 0);
			var request = client.put("/%s", cet.getCode().toLowerCase());
			client.execute(request, null);
		}
	}

	@Override
	public void removeCet(CustomEntityTemplate cet) {
		// for (var repository : cet.getRepositories()) {
		// 	ElasticsearchClient client = beginTransaction(repository, 0);
		// 	DeleteIndexRequest request = new DeleteIndexRequest.Builder()
		// 		.index(cet.getCode())
		// 		.build();

		// 	try {
		// 		client.indices().delete(request);
		// 	} catch (IOException e) {
		// 		LOG.error("Failed to delete index for {}", cet.getCode());
		// 	}
		// }
	}

	@Override
	public void crtCreated(CustomRelationshipTemplate crt) throws BusinessException {
		

	}

	@Override
	public void cftCreated(CustomModelObject template, CustomFieldTemplate cft) {
		for (var repository : template.getRepositories()) {
			ElasticRestClient client = beginTransaction(repository, 0);

			Map<String, Object> mapping = new HashMap<>();
			mapping.put("properties", 
				Map.of(cft.getCode().toLowerCase(), getPropertyFromCft(cft))
			);

			var request = client.put("/%s/_mapping", template.getCode().toLowerCase());
			client.setBody(request, JacksonUtil.toString(mapping));
			client.execute(request, null);
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
			String elasticHost = (String) cfiService.getCFValue(repository, "elasticHost");
			int elasticPort = ((Long) cfiService.getCFValue(repository, "elasticPort")).intValue();
			String elasticUsername = (String) cfiService.getCFValue(repository, "elasticUsername");
			String elasticPassword = (String) cfiService.getCFValue(repository, "elasticPassword");

			return new ElasticRestClient(elasticHost, elasticPort, elasticUsername, elasticPassword);
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
			client.close();
		});
	}

	private static Map<String, Object> getPropertyFromCft(CustomFieldTemplate cft) {
		Map<String, Object> property = new HashMap<>();

		switch (cft.getFieldType()) {
			case LONG:
				// LongNumberProperty longNumberProperty = new LongNumberProperty.Builder()
				// 	.build();
				// propertyBuilder.long_(longNumberProperty);
				// break;

			case LONG_TEXT:
			case TEXT_AREA:
				// "text"
				property.put("type", "text");
				break;
			case STRING:
				// search_as_you_type
				property.put("type", "search_as_you_type");
				break;
			default:
				break;
		}

		return property;
	}

}

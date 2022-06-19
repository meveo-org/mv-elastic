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
import javax.persistence.PersistenceException;

import org.meveo.admin.exception.BusinessException;
import org.meveo.admin.util.pagination.PaginationConfiguration;
import org.meveo.api.exception.EntityDoesNotExistsException;
import org.meveo.elastic.ElasticRestClient;
import org.meveo.model.crm.CustomFieldTemplate;
import org.meveo.model.crm.custom.CustomFieldTypeEnum;
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

import com.fasterxml.jackson.databind.JsonNode;

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

	public List<String> autoComplete(Repository repository, String cet, String cft, String query) throws BusinessException {
		ElasticRestClient client = beginTransaction(repository, 0);

		var queryJson = JacksonUtil.OBJECT_MAPPER.createObjectNode();
		queryJson.putObject("query")
			.putObject("multi_match")
			.put("query", query)
			.put("type", "phrase_prefix")
			.putArray("fields")
			.add(cft.toLowerCase())
			.add(cft.toLowerCase() + "._2gram")
			.add(cft.toLowerCase() + "._3gram");

		var request = client.get("/%s/_search", cet.toLowerCase());
		client.setBody(request, queryJson.toString());

		LOG.info("Autocomplete query = {}", queryJson);

		return client.execute(request, response -> {
			var responseJson = JacksonUtil.OBJECT_MAPPER.readTree(response.getEntity().getContent());
			return responseJson.get("hits").get("hits").findValuesAsText(cft.toLowerCase());
		}, "Failed to read response");
	}

	@Override
	public boolean exists(Repository repository, CustomEntityTemplate cet, String uuid) {
		ElasticRestClient client = beginTransaction(repository, 0);

		int status = client.head("/%s/_doc/%s", cet.getCode().toLowerCase(), uuid);
		LOG.info("Entity exists {} = {}", uuid, status == 200);
		return status == 200;
	}

	private String buildSearchRequest(StorageQuery query, Map<String, CustomFieldTemplate> fields) {
		var json = JacksonUtil.OBJECT_MAPPER.createObjectNode();

		var queries = json.putObject("query")
			.putObject("bool")
			.putArray("must");

		query.getFilters().forEach((filterKey, filterValue) -> {
			if (!filterKey.equals("uuid") && filterValue != null) {
				var cft = fields.get(filterKey);

				if (cft.getFieldType() == CustomFieldTypeEnum.STRING) {
					queries.addObject()
						.putObject("wildcard")
						.put(filterKey.toLowerCase(), String.valueOf(filterValue));
				} else {
					queries.addObject()
						.putObject("match")
						.putObject(filterKey.toLowerCase())
						.put("query", String.valueOf(filterValue))
						.put("fuzziness", "AUTO");
				}

			}
		});
		
		return json.toString();
	}

	@Override
	public String findEntityIdByValues(Repository repository, CustomEntityInstance cei) {
		StorageQuery query = new StorageQuery();
		query.setCet(cei.getCet());
		query.setRepository(repository);
		query.setFilters(cei.getCfValuesAsValues(storageType(), cei.getFieldTemplates().values(), true));

		try {
			var result = this.find(query);
			if (result.size() == 1) {
				return (String) result.get(0).get("uuid");
			} else if (result.size() > 1) {
				throw new PersistenceException("Many possible entity for values " + query.getFilters().toString());
			}

		} catch (EntityDoesNotExistsException e) {
			throw new PersistenceException("Template does not exists", e);
		}

		return null;
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
			var responseJson =  JacksonUtil.OBJECT_MAPPER.readTree(response.getEntity().getContent());
			LOG.info("Find by id {} = {}", uuid, responseJson);
			return mapHitToCfts(responseJson, cfts.values());
		}, (e) -> {
			LOG.error("Failed to read response", e);
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
			var responseJson =  mapper.readTree(response.getEntity().getContent());
			LOG.info("Search result = " + responseJson);
			
			var hits = responseJson.get("hits").get("hits");

			return StreamSupport.stream(hits.spliterator(), false)
				.map(node -> mapHitToCfts(node, fieldsTemplates.values()))
				.collect(Collectors.toList());

		}, (e) -> {
			LOG.error("Failed to read response", e);
		});
	}

	private String getDocBody(CustomEntityInstance cei) {
		Map<String, Object> body = new HashMap<>();

		cei.getValues(storageType())
			.forEach((key, value) -> {
				body.put(key.toLowerCase(), value);
			});

		return JacksonUtil.toString(body);
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
			client.setBody(put, getDocBody(cei));
			return client.execute(put, response -> {
				var json = JacksonUtil.OBJECT_MAPPER.readTree(response.getEntity().getContent());
				LOG.info("Create response = {}", json);
				if (json.get("result").asText().equals("created")) {
					return new PersistenceActionResult(json.get("_id").asText());
				} else {
					throw new PersistenceException("Elastic response : " + json.toString());
				}
			}, "Failed to create / update data");
		}
	}

	@Override
	public PersistenceActionResult addCRTByUuids(Repository repository, CustomRelationshipTemplate crt,
			Map<String, Object> relationValues, String sourceUuid, String targetUuid) throws BusinessException {
		return null;
	}

	@Override
	public void update(Repository repository, CustomEntityInstance cei) throws BusinessException {
		ElasticRestClient client = beginTransaction(repository, 0);
		var request = client.put("/%s/_doc/%s", cei.getCetCode().toLowerCase(), cei.getUuid());
		client.setBody(request, getDocBody(cei));

		boolean result = client.execute(request, response -> {
			JsonNode json = null;
			try {
				json = JacksonUtil.OBJECT_MAPPER.readTree(response.getEntity().getContent());
			} catch (UnsupportedOperationException | IOException e) {
				return false;
			}

			if (json.get("result").asText().equals("updated")) {
				return true;
			} else {
				return false;
			}
		});

		if (!result) {
			throw new BusinessException("Failed to update");
		}
	}

	@Override
	public void setBinaries(Repository repository, CustomEntityTemplate cet, CustomFieldTemplate cft, String uuid,
			List<File> binaries) throws BusinessException {

	}

	@Override
	public void remove(Repository repository, CustomEntityTemplate cet, String uuid) throws BusinessException {
		ElasticRestClient client = beginTransaction(repository, 0);
		client.delete("/%s/_doc/%s", cet.getCode().toLowerCase(), uuid);
	}

	@Override
	public Integer count(Repository repository, CustomEntityTemplate cet, PaginationConfiguration paginationConfiguration) {
		final Map<String, Object> filters = paginationConfiguration == null ? null : paginationConfiguration.getFilters();

		StorageQuery query = new StorageQuery();
		query.setCet(cet);
		query.setFilters(filters);
		query.setPaginationConfiguration(paginationConfiguration);
		query.setRepository(repository);

		ElasticRestClient client = beginTransaction(query.getRepository(), 0);
		var fieldsTemplates = cftService.findByAppliesTo(query.getCet().getAppliesTo());

		var get = client.get("/%s/_count", query.getCet().getCode().toLowerCase());
		client.setBody(get, buildSearchRequest(query, fieldsTemplates));

		return client.execute(get, response -> {
			var mapper = JacksonUtil.OBJECT_MAPPER;
			var responseJson =  mapper.readTree(response.getEntity().getContent());
			LOG.info("Count result = {}", responseJson);
			return responseJson.get("count").asInt();
		}, e -> { 
			LOG.error("Failed to read response", e);
		});
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
		for (var repository : cet.getRepositories()) {
			ElasticRestClient client = beginTransaction(repository, 0);
			int result = client.delete("/%s", cet.getCode().toLowerCase());
			if (result == 404) {
				LOG.info("Index cet {} already deleted", cet.getCode().toLowerCase());
			} else if (result == 200) {
				LOG.info("Index cet {} successfully deleted", cet.getCode().toLowerCase());
			} else {
				throw new PersistenceException("Error deleting cet index " + cet.getCode().toLowerCase());
			}
		}
	}

	@Override
	public void crtCreated(CustomRelationshipTemplate crt) throws BusinessException {
		//NOOP
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
		//NOOP - TODO later
	}

	@Override
	public void crtUpdated(CustomRelationshipTemplate cet) throws BusinessException {
		//NOOP 
	}

	@Override
	public void cftUpdated(CustomModelObject template, CustomFieldTemplate oldCft, CustomFieldTemplate cft) {
		//NOOP - TODO later
	}

	@Override
	public void removeCft(CustomModelObject template, CustomFieldTemplate cft) {
		//NOOP
	}

	@Override
	public void removeCrt(CustomRelationshipTemplate crt) {
		//NOOP
	}

	@Override
	public void init() {
		//NOOP
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

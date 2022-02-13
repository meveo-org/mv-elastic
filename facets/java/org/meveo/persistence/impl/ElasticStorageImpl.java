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

import javax.enterprise.context.RequestScoped;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.meveo.admin.exception.BusinessException;
import org.meveo.admin.util.pagination.PaginationConfiguration;
import org.meveo.api.exception.EntityDoesNotExistsException;
import org.meveo.model.crm.CustomFieldTemplate;
import org.meveo.model.customEntities.CustomEntityInstance;
import org.meveo.model.customEntities.CustomEntityTemplate;
import org.meveo.model.customEntities.CustomModelObject;
import org.meveo.model.customEntities.CustomRelationshipTemplate;
import org.meveo.model.storage.Repository;
import org.meveo.persistence.PersistenceActionResult;
import org.meveo.persistence.StorageImpl;
import org.meveo.persistence.StorageQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RequestScoped
public class ElasticStorageImpl implements StorageImpl {

	private Map<String, RestHighLevelClient> clients = new ConcurrentHashMap<String, RestHighLevelClient>();
	
	private static Logger LOG = LoggerFactory.getLogger(ElasticStorageImpl.class);

	@Override
	public boolean exists(Repository repository, CustomEntityTemplate cet, String uuid) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String findEntityIdByValues(Repository repository, CustomEntityInstance cei) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, Object> findById(Repository repository, CustomEntityTemplate cet, String uuid, Map<String, CustomFieldTemplate> cfts, Collection<String> fetchFields, boolean withEntityReferences) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Map<String, Object>> find(StorageQuery query) throws EntityDoesNotExistsException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PersistenceActionResult createOrUpdate(Repository repository, CustomEntityInstance cei, Map<String, CustomFieldTemplate> customFieldTemplates, String foundUuid) throws BusinessException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PersistenceActionResult addCRTByUuids(Repository repository, CustomRelationshipTemplate crt, Map<String, Object> relationValues, String sourceUuid, String targetUuid) throws BusinessException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void update(Repository repository, CustomEntityInstance cei) throws BusinessException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setBinaries(Repository repository, CustomEntityTemplate cet, CustomFieldTemplate cft, String uuid, List<File> binaries) throws BusinessException {
		// TODO Auto-generated method stub

	}

	@Override
	public void remove(Repository repository, CustomEntityTemplate cet, String uuid) throws BusinessException {
		// TODO Auto-generated method stub

	}

	@Override
	public Integer count(Repository repository, CustomEntityTemplate cet, PaginationConfiguration paginationConfiguration) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void cetCreated(CustomEntityTemplate cet) {
		for (var repository : cet.getRepositories()) {
			RestHighLevelClient client = beginTransaction(repository, 0);
			CreateIndexRequest request = Requests.createIndexRequest(cet.getCode());
			
			try {
				client.indices().create(request, RequestOptions.DEFAULT);
			} catch (IOException e) {
				LOG.error("Failed to create index for {}", cet.getCode());
			}
		}
	}
	
	@Override
	public void removeCet(CustomEntityTemplate cet) {
		for (var repository : cet.getRepositories()) {
			RestHighLevelClient client = beginTransaction(repository, 0);
			DeleteIndexRequest request = Requests.deleteIndexRequest(cet.getCode());
			
			try {
				client.indices().delete(request, RequestOptions.DEFAULT);
			} catch (IOException e) {
				LOG.error("Failed to delete index for {}", cet.getCode());
			}
		}
	}

	@Override
	public void crtCreated(CustomRelationshipTemplate crt) throws BusinessException {
		// TODO Auto-generated method stub

	}

	@Override
	public void cftCreated(CustomModelObject template, CustomFieldTemplate cft) {
		for (var repository : template.getRepositories()) {
			RestHighLevelClient client = beginTransaction(repository, 0);
			PutMappingRequest request = new PutMappingRequest(template.getCode());
			
			Map<String, String> mapping = new HashMap<>();
			request.source(clients);


				
			
//			try {
//				client.indices().delete(request, RequestOptions.DEFAULT);
//			} catch (IOException e) {
//				LOG.error("Failed to delete index for {}", cet.getCode());
//			}
		}

	}

	@Override
	public void cetUpdated(CustomEntityTemplate oldCet, CustomEntityTemplate cet) {
		// TODO Auto-generated method stub

	}

	@Override
	public void crtUpdated(CustomRelationshipTemplate cet) throws BusinessException {
		// TODO Auto-generated method stub

	}

	@Override
	public void cftUpdated(CustomModelObject template, CustomFieldTemplate oldCft, CustomFieldTemplate cft) {
		// TODO Auto-generated method stub

	}

	@Override
	public void removeCft(CustomModelObject template, CustomFieldTemplate cft) {
		// TODO Auto-generated method stub

	}

	@Override
	public void removeCrt(CustomRelationshipTemplate crt) {
		// TODO Auto-generated method stub

	}

	@Override
	public void init() {

	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T beginTransaction(Repository repository, int stackedCalls) {
		return (T) clients.computeIfAbsent(repository.getCode(),code -> {
			var cfValues = repository.getCfValues();
			String elasticUrl = cfValues.getCfValue("elasticUrl").getStringValue();
			String elasticUsername = cfValues.getCfValue("elasticUsername").getStringValue();
			String elasticPassword = cfValues.getCfValue("elasticPassword").getStringValue();
			
			//TODO: Decrypt password
			var httpHost = HttpHost.create(elasticUrl);
			
			var clientBuilder = RestClient.builder(httpHost);
			applyAuthentication(clientBuilder, elasticUsername, elasticPassword);
			
			return new RestHighLevelClient(clientBuilder);
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
				client.close();
			} catch (IOException e) {
				LOG.error("Failed to close elastic client", e);
			}
		});
	}

	private void applyAuthentication(RestClientBuilder restClientBuilder, String user, String password) {
		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password));
		restClientBuilder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
			@Override
			public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
				return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
			}
		});
	}

}

package org.meveo.endpoints;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.meveo.admin.exception.BusinessException;
import org.meveo.api.rest.technicalservice.EndpointScript;
import org.meveo.persistence.impl.ElasticStorageImpl;
import org.meveo.service.storage.RepositoryService;

public class AutoCompleteEndpointScript extends EndpointScript {

	@Inject
	private ElasticStorageImpl storageImpl;

	@Inject
	private RepositoryService repositoryService;

	private String repository = "default";

	private String entity;

	private String field;

	private String query;

	private List<String> output;
	
	@Override
	public void execute(Map<String, Object> methodContext) throws BusinessException {
		var repo = repositoryService.findByCode(repository);
		output = storageImpl.autoComplete(repo, entity, field, query);
	}

	public void setRepository(String repository) {
		this.repository = repository;
	}

	public void setEntity(String entity) {
		this.entity = entity;
	}

	public void setField(String field) {
		this.field = field;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public List<String> getOutput() {
		return output;
	}

}

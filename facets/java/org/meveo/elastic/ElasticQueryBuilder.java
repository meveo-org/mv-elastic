package org.meveo.elastic;

import java.util.Map;

import com.fasterxml.jackson.databind.node.*;

import org.meveo.service.script.Script;
import org.meveo.admin.exception.BusinessException;
import org.meveo.model.persistence.JacksonUtil;

public class ElasticQueryBuilder extends Script {

    private int pageSize;
    private int pageNumber;

    private ObjectNode keywordNode;
    private ObjectNode filter;
    private ObjectNode isAvailableNode;
    private ObjectNode priceRangeNode;
	
	@Override
	public void execute(Map<String, Object> parameters) throws BusinessException {
		super.execute(parameters);
	}

    public ElasticQueryBuilder withPageSize(int pageSize) {
        this.pageSize = pageSize;
        return this;
    }

    public ElasticQueryBuilder withPageNumber(int pageNumber) {
        this.pageNumber = pageNumber;
        return this;
    }

    public ElasticQueryBuilder withKeyword(String keyword) {
        keywordNode = JacksonUtil.OBJECT_MAPPER.createObjectNode();

        if (keyword == null || keyword.isEmpty() || keyword.trim().isEmpty()) {            
            keywordNode.putObject("match_all");
        }
        else {
            keywordNode.putObject("multi_match")
                        .put("query", keyword)
                        .put("type", "phrase_prefix")
                        .putArray("fields")
                        .add("*")
                        .add("*._2gram")
                        .add("*._3gram");
        }
        return this;
    }

    public ElasticQueryBuilder withFilterIsAvailable(Boolean isAvailable) {
        if (isAvailable == null || isAvailable == false) {
            isAvailableNode = null;
            return this;
        }

        isAvailableNode = JacksonUtil.OBJECT_MAPPER.createObjectNode();
            isAvailableNode.putObject("range")
                        .putObject("qty_available")
                        .put("gt", 0.0)
                        .put("boost", 2.0);

        return this;
    }

    public ElasticQueryBuilder withFilterPriceRange(Double min, Double max) {
        if (min == null && max == null) {
            priceRangeNode = null;
            return this;
        }

        priceRangeNode = JacksonUtil.OBJECT_MAPPER.createObjectNode();
        priceRangeNode.putObject("range")
                        .putObject("lst_price")
                        .put("gte", min)
                        .put("lte", max)
                        .put("boost", 2.0);

        return this;
    }

    public String getFilters() {
        filter = JacksonUtil.OBJECT_MAPPER.createObjectNode();
        ArrayNode filters = filter.putArray("must");

        if (isAvailableNode != null) {
            filters.add(isAvailableNode);
        }

        if (priceRangeNode != null) {
            filters.add(priceRangeNode);
        }

        return filter.toString();
    }

    public String build() {
        var queryJsonStr = 
        "{"
        + "    \"from\": " + pageNumber + ","
        + "    \"size\": " + pageSize + ","
        + "    \"query\": {"
        + "        \"bool\": {"
        + "            \"must\": "
        + "                " + keywordNode.toString() + ","
        + "            \"filter\": ["
        + "                {"
        + "                    \"bool\": " + getFilters()
        + "                }"
        + "            ]"
        + "        }"
        + "    }"
        + "}";

        return queryJsonStr;
    }
	
}
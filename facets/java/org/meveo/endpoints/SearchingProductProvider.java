package org.meveo.endpoints;

import java.util.Map;
import java.util.List;
import java.lang.String;
import javax.inject.Inject;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;
import org.meveo.service.script.Script;
import org.meveo.admin.exception.BusinessException;
import org.meveo.commons.utils.ParamBean;
import org.meveo.commons.utils.ParamBeanFactory;
import org.meveo.model.persistence.JacksonUtil;
import org.meveo.elastic.ElasticQueryBuilder;
import org.meveo.elastic.ElasticRestClient;

public class SearchingProductProvider extends Script {

    private ElasticQueryBuilder queryBuilder = new ElasticQueryBuilder();
    
    @Inject
    private ParamBeanFactory paramBeanFactory;

    private String _protocol;
    private String _host;
    private String _port;
    private String _username;
    private String _password;

    private String indexName;

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    private String keyword;

    public void setKeyword(String keyword) {
        this.keyword = keyword;
    }

    private String name;

    private Integer[] productCategsIds;

    private String orderBy;

    private String ordering;

    private Double longitude_user;

    private Double latitude_user;

    private Integer pageSize = 0;

    private Integer currentPage = 0;

    private Double priceMin;

    private Double priceMax;

    private Boolean isBestSeller = false;

    private Double locationRadiusInKm = 0.0;
    
    private Boolean isAvailable;

    private Integer ratingMin;

    public void setLocationRadiusInKm(Double locationRadiusInKm) {
        this.locationRadiusInKm = locationRadiusInKm;
    }

    public void setIsAvailable(Boolean isAvailable) {
        this.isAvailable = isAvailable;
    }

    public void setIsBestSeller(Boolean isBestSeller) {
        this.isBestSeller = isBestSeller;
    }

    public void setPriceMin(Double priceMin) {
        this.priceMin = priceMin;
    }

    public void setPriceMax(Double priceMax) {
        this.priceMax = priceMax;
    }

    public void setRatingMin(Integer ratingMin) {
        this.ratingMin = ratingMin;
    }

    public void setLongitude_user(Double longitude_user) {
        this.longitude_user = longitude_user;
    }

    public void setLatitude_user(Double latitude_user) {
        this.latitude_user = latitude_user;
    }

    public void setProductCategsIds(Integer[] productCategsIds) {
        this.productCategsIds = productCategsIds;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setOrderBy(String orderBy) {
        this.orderBy = orderBy;
    }

    public void setOrdering(String ordering) {
        this.ordering = ordering;
    }

    public void setCurrentPage(Integer currentPage) {
        this.currentPage = currentPage;
    }

    public void setPageSize(Integer pageSize) {
        this.pageSize = pageSize;
    }

    private String result;

    public String getResult() {
        return result;
    }

    private void init() {
        ParamBean config = paramBeanFactory.getInstance();
        _protocol = config.getProperty("elasticsearch.protocol");
        _host     = config.getProperty("elasticsearch.hosts");
        _port     = config.getProperty("elasticsearch.port");
        _username = config.getProperty("elasticsearch.username");
        _password = config.getProperty("elasticsearch.password");
    }

    @Override
    public void execute(Map<String, Object> parameters) throws BusinessException {
        super.execute(parameters);
        this.init();

        try {
            result = this.query(currentPage, pageSize, keyword, indexName, "*");
        } catch (Exception e) {
            result = "{\"error\": \""+e+"\"}";
        }
    }

    private String query(int pageNumber, int pageSize, String keyword, String indexName, String fields) throws BusinessException {
        var client = new ElasticRestClient(_protocol + "://" + _host, Integer.parseInt(_port), _username, _password);
        
        var request = client.get("/%s/_search", indexName.toLowerCase());

        var query = generateQuery(pageNumber, pageSize, keyword, indexName, fields);
        
        client.setBody(request, query);

        String content = client.execute(
            request, 
            response -> {
                return EntityUtils.toString(response.getEntity(), "UTF-8");
            },
            "{\"error\": \"Failed to read response\"}");
        return content;
    }

    public String generateQuery(int pageNumber, int pageSize, String searchKeyword, String indexName, String fields) {
        String query = queryBuilder.withKeyword(searchKeyword)
                                    .withFilterIsAvailable(isAvailable)
                                    .withFilterPriceRange(priceMin, priceMax)                                    
                                    .withPageSize(pageSize)
                                    .withPageNumber(pageNumber)
                                    .build();
        return query;
    }
    
}

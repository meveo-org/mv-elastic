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
import org.meveo.elastic.ElasticRestClient;

public class SearchingProductProvider extends Script {   
    
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

    private int pageSize = 10;

    private int currentPage = 1;

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

    public void setCurrentPage(int currentPage) {
        this.currentPage = currentPage;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    private JSONObject result;

    public JSONObject getResult() {
        return result;
    }

    private void init() {
        ParamBean config = paramBeanFactory.getInstance();
        _protocol = config.getProperty("elasticsearch.protocol","http");
        _host     = config.getProperty("elasticsearch.hosts","index-dev.telecelplay.io");
        _port     = config.getProperty("elasticsearch.port","9200");
        _username = config.getProperty("elasticsearch.username","elastic");
        _password = config.getProperty("elasticsearch.password","GckzEi8j9uVBW1lCMmcB");
    }

    @Override
    public void execute(Map<String, Object> parameters) throws BusinessException {
        super.execute(parameters);
        this.init();
        try {

            result = this.query(currentPage, pageSize, keyword, indexName, "*");         
          
        } catch (Exception e) {

            result = new JSONObject("{\"error\": \""+e.toString()+"\"}");
        }
    }

    public JSONObject query(int pageNumber, int pageSize, String keyword, String indexName, String fields) throws Exception {
        var client = new ElasticRestClient(_protocol + "://" + _host, Integer.parseInt(_port), _username, _password);

        var queryJson = JacksonUtil.OBJECT_MAPPER.createObjectNode();

        queryJson.put("from", pageNumber)
                .put("size", pageSize)
                .putObject("query")
                .putObject("multi_match")
                .put("query", keyword)
                .put("type", "phrase_prefix")
                .putArray("fields")
                .add(fields.toLowerCase())
                .add(fields.toLowerCase() + "._2gram")
                .add(fields.toLowerCase() + "._3gram");
        
        var request = client.get("/%s/_search", indexName.toLowerCase());
        
        client.setBody(request, queryJson.toString());

        String content = client.execute(request, response -> {
            return EntityUtils.toString(response.getEntity());
        }, "{\"error\": \"Failed to read response\"}");

        return new JSONObject(content);

    }
    
}

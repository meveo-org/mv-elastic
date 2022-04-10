/**
 * 
 */
package org.meveo.elastic;

import java.util.Map;

import org.meveo.admin.exception.BusinessException;
import org.meveo.model.crm.CustomFieldTemplate;
import org.meveo.model.persistence.DBStorageType;
import org.meveo.persistence.DBStorageTypeService;
import org.meveo.service.admin.impl.ModuleInstallationContext;
import org.meveo.service.script.ScriptInstanceService;
import org.meveo.service.script.module.ModuleScript;

public class InstallationScript extends ModuleScript {

    DBStorageTypeService dbStorageTypeService = getCDIBean(DBStorageTypeService.class);
    ScriptInstanceService scriptInstanceService = getCDIBean(ScriptInstanceService.class);
    ModuleInstallationContext installationContext = getCDIBean(ModuleInstallationContext.class);

    @Override
    public void postInstallModule(Map<String, Object> methodContext) throws BusinessException {
        // Register new storage type
        DBStorageType elasticDbStorageType = new DBStorageType();
        elasticDbStorageType.setCode("ELASTIC");
        elasticDbStorageType.setStorageImplScript(scriptInstanceService.findByCode("org.meveo.persistence.impl.ElasticStorageImpl"));
        dbStorageTypeService.create(elasticDbStorageType);
    }
}

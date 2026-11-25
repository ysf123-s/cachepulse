package com.cachepulse.cache.starter.fixture;

import java.util.List;

/** 测试用的数据访问接口，代替 demo-user-service 的 MyBatis-Plus Mapper。 */
public interface DepartmentMapper {
    Department selectById(Long id);

    List<Department> selectBatchIds(List<Long> ids);
}

package com.cachepulse.cache.starter.fixture;

import java.util.List;

/** 测试用的数据访问接口，代替 demo-user-service 的 MyBatis-Plus Mapper。 */
public interface UserMapper {
    User selectById(Long id);

    List<User> selectBatchIds(List<Long> ids);
}

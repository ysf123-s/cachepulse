package com.cachepulse.sync;

import lombok.Data;

/** 测试用的接入方实体：同步服务本身没有业务实体，只在测试里配合接入组件的查询回填使用。 */
@Data
public class User {
    private Long id;
    private String name;
}

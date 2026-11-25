package com.cachepulse.demo.user.loader;

import com.cachepulse.cache.starter.CacheDataLoader;
import com.cachepulse.demo.user.entity.Department;
import com.cachepulse.demo.user.mapper.DepartmentMapper;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.springframework.stereotype.Component;
import java.util.List;

@Component
public class DepartmentCacheLoader implements CacheDataLoader<Department> {
    private final DepartmentMapper mapper;
    public DepartmentCacheLoader(DepartmentMapper mapper) { this.mapper = mapper; }
    @Override public String table() { return "departments"; }
    @Override public Class<Department> entityType() { return Department.class; }
    @Override public Department loadById(Long id) { return mapper.selectById(id); }
    @Override public List<Department> loadByIds(List<Long> ids) { return mapper.selectBatchIds(ids); }
    @Override public Long idOf(Department entity) { return entity.getId(); }

    @Override
    public List<Long> loadIdsAfter(long afterId, int limit) {
        return mapper.selectList(new QueryWrapper<Department>().select("id").gt("id", afterId).orderByAsc("id")
                .last("LIMIT " + limit)).stream().map(Department::getId).toList();
    }
}

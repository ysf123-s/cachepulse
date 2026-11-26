package com.cachepulse.demo.user.service;

import com.cachepulse.cache.starter.BatchQueryResult;
import com.cachepulse.cache.starter.CacheQueryService;
import com.cachepulse.demo.user.dto.DepartmentWriteRequest;
import com.cachepulse.demo.user.entity.Department;
import com.cachepulse.demo.user.mapper.DepartmentMapper;

import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class DepartmentService {
    private final DepartmentMapper mapper;
    private final CacheQueryService queries;

    public DepartmentService(DepartmentMapper mapper, CacheQueryService queries) {
        this.mapper = mapper;
        this.queries = queries;
    }

    public Department getById(Long id) {
        return queries.getById("departments", id, Department.class);
    }

    public BatchQueryResult<Department> getByIds(List<Long> ids) {
        return queries.getByIds("departments", ids, Department.class);
    }

    public Long create(DepartmentWriteRequest request) {
        Department department = new Department();
        department.setName(request.name());
        department.setCode(request.code());
        mapper.insert(department);
        return department.getId();
    }

    public boolean update(Long id, DepartmentWriteRequest request) {
        UpdateWrapper<Department> update = new UpdateWrapper<>();
        update.eq("id", id).set("name", request.name()).set("code", request.code());
        return mapper.update(null, update) > 0;
    }

    public boolean delete(Long id) {
        return mapper.deleteById(id) > 0;
    }
}

package com.cachepulse.demo.user.controller;

import com.cachepulse.cache.starter.BatchQueryResult;
import com.cachepulse.demo.user.diagnostics.DepartmentWriteTiming;
import com.cachepulse.demo.user.dto.DepartmentWriteRequest;
import com.cachepulse.demo.user.dto.IdBatchRequest;
import com.cachepulse.demo.user.entity.Department;
import com.cachepulse.demo.user.service.DepartmentService;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.ObjectProvider;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.net.URI;
import java.util.Map;

@RestController
@RequestMapping("/departments")
public class DepartmentController {
    private final DepartmentService service;
    private final DepartmentWriteTiming writeTiming;

    public DepartmentController(DepartmentService service, ObjectProvider<DepartmentWriteTiming> writeTiming) {
        this.service = service;
        this.writeTiming = writeTiming.getIfAvailable();
    }

    @GetMapping("/{id}")
    public Department getById(@PathVariable Long id) {
        Department department = service.getById(id);
        if (department == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Department not found");
        }
        return department;
    }

    @PostMapping("/batch")
    public BatchQueryResult<Department> getByIds(@RequestBody IdBatchRequest request) {
        request.validate();
        return service.getByIds(request.ids());
    }

    @PostMapping
    public ResponseEntity<Map<String, Long>> create(@RequestBody DepartmentWriteRequest request) {
        validate(request);
        Long id = service.create(request);
        return ResponseEntity.created(URI.create("/departments/" + id)).body(Map.of("id", id));
    }

    @PutMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void update(@PathVariable Long id, @RequestBody DepartmentWriteRequest request,
                       @RequestHeader(name = "X-CachePulse-Write-Timing", required = false) String timingMode,
                       HttpServletResponse response) {
        validate(request);
        boolean updated;
        if (writeTiming != null && ("auto".equals(timingMode) || "explicit".equals(timingMode))) {
            DepartmentWriteTiming.Result result = writeTiming.measure("explicit".equals(timingMode),
                    () -> service.update(id, request));
            response.setHeader("Server-Timing", result.header());
            updated = result.updated();
        } else {
            updated = service.update(id, request);
        }
        if (!updated) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Department not found");
        }
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void delete(@PathVariable Long id) {
        if (!service.delete(id)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Department not found");
        }
    }

    private void validate(DepartmentWriteRequest request) {
        if (request.name() == null || request.name().isBlank() || request.name().length() > 64) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "name must be non-blank and at most 64 characters");
        }
        if (request.code() != null && request.code().length() > 32) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "code must be at most 32 characters");
        }
    }
}

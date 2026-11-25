package com.cachepulse.demo.user.service;

import com.cachepulse.demo.user.dto.UserWriteRequest;
import com.cachepulse.demo.user.entity.User;
import com.cachepulse.demo.user.mapper.UserMapper;

import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import org.springframework.stereotype.Service;

/**
 * 只写数据库，缓存失效交给现有 Canal → Kafka 消费链路。
 */
@Service
public class UserWriteService {
    private final UserMapper userMapper;

    public UserWriteService(UserMapper userMapper) {
        this.userMapper = userMapper;
    }

    public Long create(UserWriteRequest request) {
        User user = new User();
        user.setName(request.name());
        user.setEmail(request.email());
        user.setPhone(request.phone());
        userMapper.insert(user);
        return user.getId();
    }

    public boolean update(Long id, UserWriteRequest request) {
        // 显式 set，确保可选字段为 null 时也会写入 SQL，而不是被跳过。
        UpdateWrapper<User> update = new UpdateWrapper<>();
        update.eq("id", id)
                .set("name", request.name())
                .set("email", request.email())
                .set("phone", request.phone());
        return userMapper.update(null, update) > 0;
    }

    public boolean delete(Long id) {
        return userMapper.deleteById(id) > 0;
    }
}

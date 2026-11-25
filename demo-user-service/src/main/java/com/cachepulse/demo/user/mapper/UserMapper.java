package com.cachepulse.demo.user.mapper;

import com.cachepulse.demo.user.entity.User;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface UserMapper extends BaseMapper<User> {
}

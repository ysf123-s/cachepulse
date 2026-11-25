package com.cachepulse.demo.user.dto;

/** PUT 整体替换资料；code 省略或为 null 时清空。 */
public record DepartmentWriteRequest(String name, String code) {
}

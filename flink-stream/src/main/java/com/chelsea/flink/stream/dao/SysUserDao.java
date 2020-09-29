package com.chelsea.flink.stream.dao;

import java.util.List;

import com.chelsea.flink.stream.domain.SysUser;

public interface SysUserDao {

    List<SysUser> querySysUser();

}

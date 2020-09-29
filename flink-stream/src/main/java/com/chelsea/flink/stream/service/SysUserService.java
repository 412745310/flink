package com.chelsea.flink.stream.service;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.chelsea.flink.stream.dao.SysUserDao;
import com.chelsea.flink.stream.domain.SysUser;

@Service
public class SysUserService {
    
    @Autowired
    private SysUserDao sysUser;
    
    public List<SysUser> querySysUser() {
        return sysUser.querySysUser();
    }

}

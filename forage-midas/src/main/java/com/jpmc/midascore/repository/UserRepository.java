package com.jpmc.midascore.repository;

import org.springframework.data.repository.CrudRepository;

import com.jpmc.midascore.entity.UserRecord;

public interface UserRepository extends CrudRepository<UserRecord, Long> {
}

package com.fan.nanwang.repository;

import com.fan.nanwang.entity.PowerPlant;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PowerPlantRepository extends JpaRepository<PowerPlant, Long> {

    PowerPlant findByNameAbbreviation(String nameAbbreviation);
}

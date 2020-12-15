package com.example.esdemo.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.springframework.data.relational.core.mapping.Table;

import java.io.Serializable;

/**
 * @author P52
 */
@Accessors(chain = true)
@Data
@AllArgsConstructor
@NoArgsConstructor
@Table("/object")
public class ObjectEntity implements Serializable {

    private String id;

    private String userName;

    private String password;

}

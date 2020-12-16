package com.example.esdemo.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.io.Serializable;

/**
 * @author P52
 */
@Accessors(chain = true)
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ObjectVo implements Serializable {

    private String id;

    private String userName;

    private String password;
}

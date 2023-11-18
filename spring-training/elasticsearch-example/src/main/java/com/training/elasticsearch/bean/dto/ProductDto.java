package com.training.elasticsearch.bean.dto;

import lombok.*;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.io.Serializable;

/**
 *
 * @program: spring-training
 * @description:
 * @author: cyxhc
 * @Date: 2023/11/18 18:59
 **/
@ToString
@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
@Document(indexName = "ec", type = "product", replicas = 0, shards = 5)
public class ProductDto implements Serializable {
    /**
     * id
     */
    private Long id;

    /**
     * 名称
     * 使用中文分词器
     * 这里如果不使用分词器，查找中文的时候，会出现可以查找一个字的有结果，多个字查询不出来
     */
    @Field(type = FieldType.Text, analyzer = "ik_max_word")
    private String name;

    /**
     * 数量
     */
    private Integer count;

    /**
     * 价格
     */
    private Double price;
}

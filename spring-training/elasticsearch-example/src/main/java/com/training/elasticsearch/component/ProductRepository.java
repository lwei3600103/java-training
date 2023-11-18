package com.training.elasticsearch.component;

import com.training.elasticsearch.bean.dto.ProductDto;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @program: spring-training
 * @description:
 * @author: cyxhc
 * @Date: 2023/11/18 19:00
 **/
@Component
public interface ProductRepository extends ElasticsearchRepository<ProductDto, Long> {
    /**
     * 通过ID查询
     *
     * @param id
     * @return
     */
    ProductDto findById(long id);

    /**
     * 通过名字查询
     * 如果数据返回的多个要用集合来接受返回数据 否则会出现查找到多条的错误
     * 只有例如id这种保证只会找到单个的情况，采用ProductDto接受返回值
     *
     * @param name
     * @return
     */
    List<ProductDto> findByName(String name);

    /**
     * 查询价格区间
     *
     * @return
     */
    List<ProductDto> findByPrice(double minPrice, double maxPrice);


    /**
     * 保存
     *
     * @param data must not be {@literal null}.
     * @return
     */
    ProductDto save(ProductDto data);


}

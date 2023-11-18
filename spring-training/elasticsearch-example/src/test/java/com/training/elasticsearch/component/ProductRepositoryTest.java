package com.training.elasticsearch.component;

import com.training.elasticsearch.bean.dto.ProductDto;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.query.FetchSourceFilter;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;

/**
 * @program: spring-training
 * @description:
 * @author: cyxhc
 * @Date: 2023/11/18 19:07
 **/
@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest
public class ProductRepositoryTest {

    private static final String[] PRODUCT_NAME_LIST = {"苹果", "香蕉", "汽车", "电话", "电视"};
    /**
     * 默认一个处理ID
     */
    private static final long NORMAL_ID = 100001L;

    /**
     * 批量插入最多数量
     */
    private static final int MAX_COUNT = 40;

    @Autowired
    private ProductRepository productRepository;

    @Test
    public void save() {
        ProductDto productDto = ProductDto.builder()
                .id(NORMAL_ID)
                .name("手机")
                .count(5)
                .price(1999.45).build();

        productRepository.save(productDto);
    }

    @Test
    public void saveAll() {
        Random random = new Random();
        random.setSeed(300);
        List<ProductDto> list = new ArrayList<>(MAX_COUNT);
        long curTimeMillis = System.currentTimeMillis();
        for (int i = 0; i < 40; i++) {
            double interval = random.nextDouble();
            ProductDto productDto = ProductDto.builder()
                    //加上random，是为了区分id，否则System.currentTimeMillis()返回的是同一个值
                    .id(curTimeMillis + i)
                    .name(PRODUCT_NAME_LIST[i % 5] + curTimeMillis)
                    .count(i)
                    .price(1500 + interval).build();
            list.add(productDto);
        }

        //底层提供方法
        productRepository.saveAll(list);
    }


    @Test
    public void update() {
        long id = 100001L;
        ProductDto productDto = ProductDto.builder()
                .id(id)
                .name("手机")
                .count(5)
                .price(2000.45).build();

        productRepository.save(productDto);
    }

    @Test
    public void findByName() {
        //如果数据返回的多个要用集合来接受返回数据 否则会出现查找到多条的错误
        List<ProductDto> productDtoList = productRepository.findByName("手机");
        productDtoList.forEach(v -> log.info("FindByName Item: {}", v));
    }

    @Test
    public void findById() {
        //ProductDto 必须要有无参构造函数，否则出现错误：ElasticsearchException: failed to map source
        ProductDto productDto = productRepository.findById(NORMAL_ID);
        log.info("findById(): {}", productDto);
    }


    @Test
    public void deleteById() {
        productRepository.deleteById(NORMAL_ID);
    }

    @Test
    public void findByList() {
        Iterable<ProductDto> list = productRepository.findAll();
        list.forEach(v -> log.info("Item: {}", v));
    }


    /**
     * 精确匹配测试
     */
    @Test
    public void testTermQuery() {
        //精确查询
        //value中只能带一个汉字，多个汉字无法查询出来
        //如果需要进行多个汉字的查询，需要在name后面加上.keyword
        //英文不受影响
        QueryBuilder queryBuilder = QueryBuilders.termQuery("name", "苹");
        NativeSearchQueryBuilder nativeSearchQueryBuilder = new NativeSearchQueryBuilder().withQuery(queryBuilder);
        Page<ProductDto> page = productRepository.search(nativeSearchQueryBuilder.build());
        for (ProductDto item : page) {
            log.info("精确查询单匹配 Item: {}", item);
        }

        queryBuilder = QueryBuilders.termQuery("name.keyword", "电视");
        nativeSearchQueryBuilder = new NativeSearchQueryBuilder().withQuery(queryBuilder);
        page = productRepository.search(nativeSearchQueryBuilder.build());
        for (ProductDto item : page) {
            log.info("精确查询单匹配keyword Item: {}", item);
        }

        //多个匹配
        nativeSearchQueryBuilder.withQuery(QueryBuilders.termsQuery("name", "苹", "果"));
        page = productRepository.search(nativeSearchQueryBuilder.build());
        for (ProductDto item : page) {
            log.info("精确查询多匹配 Item: {}", item);
        }

        //multiMatchQuery 进行多字段的精确匹配
        nativeSearchQueryBuilder.withQuery(QueryBuilders.multiMatchQuery("39", "name", "count"));
        page = productRepository.search(nativeSearchQueryBuilder.build());
        for (ProductDto item : page) {
            log.info("多字段精确查询多匹配 Item: {}", item);
        }

    }

    /**
     * 模糊匹配
     */
    @Test
    public void testQuery() {
        NativeSearchQueryBuilder nativeSearchQueryBuilder = new NativeSearchQueryBuilder();

        //1 进行单字段的模糊查询
        //字段上没有使用分词器会导致多个中文无法查出数据
        //中文字符要注意使用中文分词器
        QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("苹果").field("name");
        nativeSearchQueryBuilder.withQuery(queryBuilder);
        Page<ProductDto> page = productRepository.search(nativeSearchQueryBuilder.build());
        for (ProductDto item : page) {
            log.info("左右模糊 Item: {}", item);
        }

        //2 前缀查询
        //中文字符要注意使用中文分词器
        queryBuilder = QueryBuilders.prefixQuery("name", "苹果");
        nativeSearchQueryBuilder.withQuery(queryBuilder);
        page = productRepository.search(nativeSearchQueryBuilder.build());
        for (ProductDto item : page) {
            log.info("前缀查询 Item: {}", item);
        }
    }


    /**
     * 通配符查询
     */
    @Test
    public void testWildcardQuery() {
        NativeSearchQueryBuilder nativeSearchQueryBuilder = new NativeSearchQueryBuilder();
        QueryBuilder queryBuilder = QueryBuilders.wildcardQuery("name", "香*");
        nativeSearchQueryBuilder.withQuery(queryBuilder);
        Page<ProductDto> page = productRepository.search(nativeSearchQueryBuilder.build());
        for (ProductDto item : page) {
            log.info("通配符查询 * Item: {}", item);
        }

        //效果等同于 like '香?2' 而不是 = '香?2'
        queryBuilder = QueryBuilders.wildcardQuery("name", "香?2");
        nativeSearchQueryBuilder.withQuery(queryBuilder);
        page = productRepository.search(nativeSearchQueryBuilder.build());
        for (ProductDto item : page) {
            log.info("通配符查询 ? Item: {}", item);
        }
    }

    /**
     * 模糊查询Fuzzy
     */
    @Test
    public void testFuzzyQuery() {
        //个人理解: fuzziness(Fuzziness.TWO)后面的数字表示允许错几个单词
        NativeSearchQueryBuilder nativeSearchQueryBuilder = new NativeSearchQueryBuilder();
        QueryBuilder queryBuilder = QueryBuilders.fuzzyQuery("name", "苹查查果").fuzziness(Fuzziness.TWO);
        nativeSearchQueryBuilder.withQuery(queryBuilder);
        Page<ProductDto> page = productRepository.search(nativeSearchQueryBuilder.build());
        for (ProductDto item : page) {
            log.info("模糊查询Fuzzy Item: {}", item);
        }
    }

    /**
     * 范围查询
     */
    @Test
    public void testRangeQuery() {
        NativeSearchQueryBuilder nativeSearchQueryBuilder = new NativeSearchQueryBuilder();
        QueryBuilder queryBuilder = QueryBuilders.rangeQuery("count").from(10).to(30);
        nativeSearchQueryBuilder.withQuery(queryBuilder);
        Page<ProductDto> page = productRepository.search(nativeSearchQueryBuilder.build());
        for (ProductDto item : page) {
            log.info("范围查询 闭区间: {}", item);
        }

        queryBuilder = QueryBuilders.rangeQuery("count").from(10).to(30).includeUpper(false).includeLower(false);
        nativeSearchQueryBuilder.withQuery(queryBuilder);
        page = productRepository.search(nativeSearchQueryBuilder.build());
        for (ProductDto item : page) {
            log.info("范围查询 开区间: {}", item);
        }

        queryBuilder = QueryBuilders.rangeQuery("count").gt(10);
        nativeSearchQueryBuilder.withQuery(queryBuilder);
        page = productRepository.search(nativeSearchQueryBuilder.build());
        for (ProductDto item : page) {
            log.info("范围查询 大于: {}", item);
        }

        queryBuilder = QueryBuilders.rangeQuery("count").lte(10);
        nativeSearchQueryBuilder.withQuery(queryBuilder);
        page = productRepository.search(nativeSearchQueryBuilder.build());
        for (ProductDto item : page) {
            log.info("范围查询 小于等于: {}", item);
        }
    }


    /**
     * 组合查询
     */
    @Test
    public void testMustQuery() {
        NativeSearchQueryBuilder nativeSearchQueryBuilder = new NativeSearchQueryBuilder();
        QueryBuilder queryBuilder1 = QueryBuilders.rangeQuery("count").gt(10);
        QueryBuilder queryBuilder2 = QueryBuilders.rangeQuery("count").lte(34);
        QueryBuilder queryBuilder3 = QueryBuilders.boolQuery().must(queryBuilder1).must(queryBuilder2);
        nativeSearchQueryBuilder.withQuery(queryBuilder3);
        Page<ProductDto> page = productRepository.search(nativeSearchQueryBuilder.build());
        for (ProductDto item : page) {
            log.info("组合查询 must must: {}", item);
        }

        queryBuilder3 = QueryBuilders.boolQuery().must(queryBuilder1).mustNot(queryBuilder2);
        nativeSearchQueryBuilder.withQuery(queryBuilder3);
        page = productRepository.search(nativeSearchQueryBuilder.build());
        for (ProductDto item : page) {
            log.info("组合查询 must mustNot: {}", item);
        }

        queryBuilder3 = QueryBuilders.boolQuery().should(queryBuilder1).should(queryBuilder2);
        nativeSearchQueryBuilder.withQuery(queryBuilder3);
        page = productRepository.search(nativeSearchQueryBuilder.build());
        for (ProductDto item : page) {
            log.info("组合查询 should should: {}", item);
        }

        queryBuilder3 = QueryBuilders.boolQuery().filter(queryBuilder1);
        nativeSearchQueryBuilder.withQuery(queryBuilder3);
        page = productRepository.search(nativeSearchQueryBuilder.build());
        for (ProductDto item : page) {
            log.info("组合查询 filter: {}", item);
        }
    }

    /**
     * 分页和排序
     */
    @Test
    public void testPageAndSortedQuery() {
        //注意分页的开始页是0开始
        NativeSearchQueryBuilder nativeSearchQueryBuilder = new NativeSearchQueryBuilder();
        QueryBuilder queryBuilder1 = QueryBuilders.rangeQuery("count").gt(10);
        QueryBuilder queryBuilder2 = QueryBuilders.rangeQuery("count").lte(34);
        QueryBuilder queryBuilder3 = QueryBuilders.boolQuery().must(queryBuilder1).must(queryBuilder2);
        nativeSearchQueryBuilder.withQuery(queryBuilder3).withPageable(PageRequest.of(1, 4));
        Page<ProductDto> page = productRepository.search(nativeSearchQueryBuilder.build());
        for (ProductDto item : page) {
            log.info("分页和排序 分页: {}", item);
        }

        //注意分页的开始页是0开始
        nativeSearchQueryBuilder.withQuery(queryBuilder3)
                .withPageable(PageRequest.of(0, 4))
                .withSort(SortBuilders.fieldSort("count").order(SortOrder.DESC));
        page = productRepository.search(nativeSearchQueryBuilder.build());
        for (ProductDto item : page) {
            log.info("分页和排序 排序: {}", item);
        }
    }


    /**
     * 聚合查询
     */
    @Test
    public void testAggregationQuery() {
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        queryBuilder.withSourceFilter(new FetchSourceFilter(new String[]{""}, null));
        queryBuilder.withSearchType(SearchType.QUERY_THEN_FETCH);
        queryBuilder.withIndices("ec").withTypes("product");
        //指定通过count聚合成counts
        queryBuilder.addAggregation(AggregationBuilders.terms("counts").size(30).field("count"));
        queryBuilder.withPageable(PageRequest.of(0, 30));
        AggregatedPage<ProductDto> aggPage = (AggregatedPage<ProductDto>) productRepository.search(queryBuilder.build());

        LongTerms stringTerms = (LongTerms) aggPage.getAggregation("counts");
        List<LongTerms.Bucket> buckets = (List<LongTerms.Bucket>) stringTerms.getBuckets();
        for (LongTerms.Bucket item : buckets) {
            log.info("Key: {}, Count: {}", item.getKeyAsString(), item.getDocCount());
        }

        queryBuilder.addAggregation(AggregationBuilders.terms("count_price").size(30).field("count")
                .subAggregation(AggregationBuilders.avg("price_avg").field("price")));
        queryBuilder.withPageable(PageRequest.of(0, 30));
        aggPage = (AggregatedPage<ProductDto>) productRepository.search(queryBuilder.build());

        stringTerms = (LongTerms) aggPage.getAggregation("count_price");
        buckets = stringTerms.getBuckets();
        for (LongTerms.Bucket item : buckets) {

            Avg aggregation = (Avg) item.getAggregations().get("price_avg");
            log.info("Key: {}, Count: {}, Price: {}", item.getKeyAsString(), item.getDocCount(), aggregation.getValue());
        }

    }
}
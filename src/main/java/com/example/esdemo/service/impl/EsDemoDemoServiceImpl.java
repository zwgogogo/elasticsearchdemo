package com.example.esdemo.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.esdemo.service.EsDemoService;
import com.example.esdemo.vo.ObjectVo;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Base64Utils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.*;
import java.util.*;
import java.util.function.BiConsumer;

@Slf4j
@Service
@Transactional(propagation = Propagation.REQUIRED)
public class EsDemoDemoServiceImpl implements EsDemoService {

    /**
     * es客户端注入
     */
    @Autowired
    private RestHighLevelClient restHighLevelClient;

    /**
     * 单次提交次数
     */
    private final static int bulkTimes = 50;

    @Override
    public void addIndex(String indexName) {
        JSONObject indexSetting = new JSONObject();
        indexSetting.put("index.analysis.analyzer. default.type ", "ik_smart");
        indexSetting.put("index.number_of_shards", "6");
        indexSetting.put("index.number_of_replicas", "1");
        indexSetting.put("index.refresh_interval", "-1");
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
        String path = Objects.requireNonNull(this.getClass().getClassLoader().getResource("/mapping/plmobjectmapping.json")).getPath();
        File file = new File(path);
        List<String> strings = null;
        try {
            strings = Files.readAllLines(file.toPath());
            String mappingSet = String.join("", strings);
            log.info(String.format("mapping: %s ", mappingSet));
            createIndexRequest.settings(indexSetting.toString(), XContentType.JSON).mapping(mappingSet.toLowerCase(), XContentType.JSON);
            CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void setPipeline() {
        String path = Objects.requireNonNull(this.getClass().getClassLoader().getResource("/pipeline/plmobjectpipeline.json")).getPath();
        List<String> strings = null;
        try {
            File file = new File(path);
            strings = Files.readAllLines(file.toPath());
            log.info(String.format("pipeline: %s ", strings));
            String pipelineSett = String.join("", strings);
            PutPipelineRequest putPipelineRequest = new PutPipelineRequest("demoP", new BytesArray(pipelineSett.getBytes(StandardCharsets.UTF_8)), XContentType.JSON);
            restHighLevelClient.ingest().putPipeline(putPipelineRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Transactional(readOnly = true)
    @Override
    public void addDocumentAndFile(String indexName, String pipelineName) {
        List<Object> objectList = new ArrayList<>();
        //循环多次，其余次数的算法
        int count = 0;
        List<Object> documentListTemp = new ArrayList<>();
        for (Object object : objectList) {
            count++;
            documentListTemp.add(object);
            if (documentListTemp.size() % bulkTimes == 0) {
                this.bulkSubmit(indexName, pipelineName, documentListTemp, bulkTimes);
                documentListTemp.clear();
            }
        }
        this.bulkSubmit(indexName, pipelineName, documentListTemp, documentListTemp.size());
        log.info("all documents insert done count: " + count);

    }

    /**
     * 批量提交文档及附件
     *
     * @param indexName
     * @param pipelineName
     * @param documentList
     * @param size
     */
    private void bulkSubmit(String indexName, String pipelineName, List<Object> documentList, int size) {
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < size; i++) {
            Object document = documentList.get(i);
            IndexRequest indexRequest = new IndexRequest(indexName);
            try {
                indexRequest.id(String.valueOf(UUID.randomUUID())).setPipeline(pipelineName).create(false).source(JSON.toJSONString(document), XContentType.JSON);
                bulkRequest.add(indexRequest);
            } catch (
                    Exception e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * base64处理附件* es ingest-attachment 处理附件    base64传输数据，pipeline处理附件
     */
    private void downloadFileBase64(InputStream is) {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[1024];
            int length = 0;
            while ((length = is.read(buffer, 0, buffer.length)) > 0) {
                outputStream.write(buffer, 0, length);
                byte[] bytes = Base64Utils.encode(outputStream.toByteArray());
                String baseStr = new String(bytes);
            }
        } catch (IOException e) {
            log.warn(e.getMessage());
        }
    }

    @Transactional(readOnly = true)
    @Override
    public void addDocument(String indexName) {
        BulkProcessor bulkProcessor = getBulkProcessor();

        try {
            Class.forName("driverName");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        String docSql = null;
        try (Connection con = DriverManager.getConnection("jdbc:onacle:thin:[192.168.10.113:1521:ane.l", "username", "password");
             PreparedStatement ps = con.prepareStatement(docSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
             ResultSet rs = ps.executeQuery()
        ) {
            ResultSetMetaData metaData = rs.getMetaData();
            ArrayList<ObjectVo> dataList = new ArrayList<>();
            HashMap<String, Object> mapTemp = new HashMap<>(128);
            HashMap<String, Object> map = new HashMap<>(128);
            List<String> idListTemp = new ArrayList<>();
            List<HashMap<String, String>> ibaListTemp = new ArrayList<>();
            int submitCount = 0;
            int cycleCount = 0;
            String c;
            String v;
            boolean isCopy = false;
            while (rs.next()) {
                cycleCount++;

                for (int i = 1; i < metaData.getColumnCount() + 1; i++) {
                    c = metaData.getColumnName(i);
                    if ("id ".equalsIgnoreCase(c)) {
                        String idStr = rs.getString(c);
                        if (!idListTemp.contains(idStr)) {
                            idListTemp.clear();
                            idListTemp.add(idStr);
                            isCopy = false;
                        } else {
                            isCopy = true;
                        }
                    }
                    if (isCopy || cycleCount == 1) {
                        v = rs.getString(c);
                        map.put(c.toLowerCase(), v);
                    } else {
                        v = rs.getString(c);
                        mapTemp.put(c.toLowerCase(), v);
                    }
                }

                if (isCopy || cycleCount == 1) {
                    HashMap<String, String> ibaVoMap = new HashMap<>();
                    ibaVoMap.put("addattnid", String.valueOf(map.get("addattrid")));
                    ibaVoMap.put("addattrval", String.valueOf(map.get("addattrval")));
                    ibaVoMap.put("attrcontent", String.valueOf(map.get("title")));
                    ibaListTemp.add(ibaVoMap);
                    continue;
                } else {
                    map.put("ibavos", ibaListTemp);
                    String mapStr = JSONObject.toJSONString(map);
                    ObjectVo esVo = JSON.parseObject(mapStr, ObjectVo.class);

                    map.clear();
                    map.putAll(mapTemp);
                    mapTemp.clear();
                    ibaListTemp.clear();
                    dataList.add(esVo);

                    HashMap<String, String> ibaVoMap = new HashMap<>();
                    ibaVoMap.put("addattnid", String.valueOf(map.get("addattrid")));
                    ibaVoMap.put("addattrval", String.valueOf(map.get("addattrval")));
                    ibaVoMap.put("attrcontent", String.valueOf(map.get("title")));
                    ibaListTemp.add(ibaVoMap);
                }
                submitCount++;

                if (submitCount % 100000 == 0) {
                    for (ObjectVo obj : dataList) {
                        bulkProcessor.add(new IndexRequest(indexName).create(false).id("id").source(JSONObject.toJSONString(obj), XContentType.JSON));
                    }
                    log.info("success to insert documentNum: 100000");
                    dataList.clear();
                }
            }

            log.info(String.valueOf(dataList.size()));
            for (ObjectVo obj : dataList) {
                bulkProcessor.add(new IndexRequest(indexName).create(false).id("id").source(JSONObject.toJSONString(obj), XContentType.JSON));
            }
            bulkProcessor.flush();
        } catch (Exception e) {
            e.printStackTrace();
            log.warn(e.getMessage());
        }
    }

    /**
     * 获取批量异步处理器
     *
     * @return
     */
    private BulkProcessor getBulkProcessor() {
        BulkProcessor.Listener bulkListener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long l, BulkRequest bulkRequest) {
                log.info(" ready to insert documentNums: " + bulkRequest.numberOfActions());
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                log.info("success insert documentNums: " + bulkRequest.numberOfActions());
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
                log.warn("unSuccess insert documentId: " + l + "reason: " + throwable);
            }
        };
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer = (request, listener) -> restHighLevelClient.bulkAsync(request, RequestOptions.DEFAULT, listener);
        return BulkProcessor.builder(consumer, bulkListener)
                .setBulkActions(5000)
                .setBulkSize(new ByteSizeValue(100L, ByteSizeUnit.MB))
                .setConcurrentRequests(10)
                .setFlushInterval(TimeValue.timeValueSeconds(100L))
                .setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(1L), 3)).build();
    }

    /**
     * 搜索 DSL语句
     *
     * @param indexName
     * @return
     */
    @Override
    public Object search(String indexName) {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("", "");
        sourceBuilder.query(matchQueryBuilder);
        searchRequest.source(sourceBuilder);
        try {
            SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}



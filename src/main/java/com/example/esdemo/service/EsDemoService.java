package com.example.esdemo.service;

/**
 * 执行ES完整CRUD：使用ES官方推荐客户端：Java High Level REST Client
 *
 * @author zhangwei
 * @version beta0.1
 * @date 2020-12-15
 */
public interface EsDemoService {

    /**
     * 添加索引并增加映射。读取json文件创建索引， springboot高版本集成es,可以注解开发，缺点：可能映射不准，出现未知问题
     *
     * @param indexName
     */
    void addIndex(String indexName);

    /**
     * 新增管道操作，对数据进行预处理，此处是引用插件，ingest——attachment 使用attach操作，foreach操作
     */
    void setPipeline();

    /**
     * 新增文档及附件  附件数据base64处理传入es,进行处理
     *
     * @param indexName
     * @param pipelineName
     */
    void addDocumentAndFile(String indexName, String pipelineName);

    /**
     * 异步批量添加文档 千万级等大数据的处理，单次处理10万
     *
     * @param indexName
     */
    void addDocument(String indexName);

    /**
     * 搜索处理api
     *
     * @param indexName
     * @return
     */
    Object search(String indexName);
}

package cn.gzsxt.demo;


import cn.gzsxt.util.SolrJUtil;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * SolrJ 客户端连接Solr服务端进行CRUD
 */
public class SolrJCRUD {


    @Test
    public void clientConn() throws IOException, SolrServerException {
        //创建HttlpSolrClient对象,通过它和Solr服务器建立连接
        //参数:solr服务器的访问地址, 如需要访问某个core  就写core的名称在上面
        HttpSolrClient client = new HttpSolrClient("http://localhost:8080/solr/jd");
        //创建SolrQuery 对象
        SolrQuery query = new SolrQuery();
        //set 可以设置我们得查询条件,"q"固定且必须的查询域条件
        //如果我们不用set,我们可以使用setQuery 和set("q","xxx:xxx") 是一样效果的
        query.setQuery("product_name:金属门");
        //query.set("q","product_name:金属门");
        //client的查询方法,查询server索引库
        QueryResponse response = client.query(query);
        //查询结果
        SolrDocumentList results = response.getResults();
        //获取结果总数
        long numFound = results.getNumFound();
        System.out.println("获取总数" + numFound);
        //查询的结果是一个ArrayList的查询结果,我们可以去遍历他.
        for (SolrDocument document : results) {
            //可以直接使用coument.get 或者某些域的value
            System.out.println(document.get("product_name"));
        }
        //关闭客户端与服务端的连接
        client.close();
    }


    /**
     * 增/改,都是一样的
     * <p>
     * 添加文档进Solr索引库中
     * <p>
     * 如果,添加的文档,ID 相同并且索引库中存在,那么只会修改索引库的文档,而不会添加新的一条文档数据
     * 因为索引库添加了id 是唯一并且不为空的.必须添加ID
     *
     * @throws IOException
     * @throws SolrServerException
     */
    @Test
    public void addSolrJ() throws IOException, SolrServerException {
        //连接solr服务器
        HttpSolrClient client = SolrJUtil.getSolrClient();
        //创建SolrInputDocument对象,封装要添加的文档数据.
        SolrInputDocument document = new SolrInputDocument();
        document.addField("id", "007");
        document.addField("product_name", "007大人");
        document.addField("product_price", 18f);
        //向服务端发送文档,查询在solr服务端索引库中
        client.add(document);
        //提交
        client.commit();
        client.close();
    }


    /**
     * 删除功能
     * <p>
     * 添加删除修改 都记得commit
     *
     * @throws IOException
     * @throws SolrServerException
     */
    @Test
    public void delSolrJ() throws IOException, SolrServerException {
        //连接solr服务器
        HttpSolrClient client = SolrJUtil.getSolrClient();
        //使用DeleteById 直接查询用Id来删除对应索引库的文档
        //UpdateResponse response = client.deleteById("007");
        //或者使用DeleteByQuery ,写query查询表达式来删除对应的文档
        UpdateResponse response = client.deleteByQuery("id:007");
        System.out.println(response.getStatus());
        //记得提交
        client.commit();
        client.close();
    }


    /**
     * 复杂查询
     */
    @Test
    public void querySolrJ() throws IOException, SolrServerException {
        HttpSolrClient client = SolrJUtil.getSolrClient();

        SolrQuery query = new SolrQuery();

        query.setQuery("金属门");
        //过滤条件fq
        //query.setFilterQueries("product_catalog_name:幽默杂货");
        query.setFilterQueries("product_price:[17 TO 18.5]");
        //分页
        query.setStart(0);
        query.setRows(10);
        //排序
        query.setSort("product_price", SolrQuery.ORDER.asc);
        //设置显示的域的列表
        query.setFields("id", "product_name", "product_price",
                "product_catalog_name", "product_picture");

        //设置默认收搜域
        query.set("df", "product_name");

        //设置高亮

        query.setHighlight(true);
        query.addHighlightField("product_name");
        //高亮头
        query.setHighlightSimplePre("<em>");
        //高亮尾
        query.setHighlightSimplePost("</em>");

        QueryResponse response = client.query(query);


        SolrDocumentList results = response.getResults();

        long numFound = results.getNumFound();

        System.out.println("查询结果总数:" + numFound);


        for (SolrDocument document : results) {
            System.out.println(document.get("id"));
            String productName = (String) document.get("product_name");
            //获取高亮列表
            Map<String, Map<String, List<String>>> highlighting = response.getHighlighting();

            //获得本文档的高亮信息
            List<String> list = highlighting.get(document.get("id")).get(
                    "product_name");
            //如果有高亮，则把商品名称赋值为有高亮的那个名称
            if (list != null) {
                productName = list.get(0);
            }

            System.out.println(productName);
            System.out.println(document.get("product_price"));
            System.out.println(document.get("product_catalog_name"));
            System.out.println(document.get("product_picture"));

        }


    }


}

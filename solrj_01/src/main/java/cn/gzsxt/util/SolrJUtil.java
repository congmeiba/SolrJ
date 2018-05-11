package cn.gzsxt.util;

import org.apache.solr.client.solrj.impl.HttpSolrClient;

public class SolrJUtil {


    private static HttpSolrClient solrClient;


    public static HttpSolrClient getSolrClient(){
        return solrClient = new HttpSolrClient("http://localhost:8080/solr/jd");
    }


}

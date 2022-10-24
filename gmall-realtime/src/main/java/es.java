import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

import java.io.IOException;


/**
 * @Author wsl
 * @Date 2021-08-25
 * @Description
 * @Version 1.0
 */
public class es {
    public static void main(String[] args) throws IOException {

        //1.创建ES客户端构建器
        JestClientFactory factory = new JestClientFactory();

        //2.创建ES客户端连接地址
        HttpClientConfig httpClientConfig = new HttpClientConfig
                .Builder("http://usdp-o3tbdsfp-core3:9200")
                .build();
        factory.setHttpClientConfig(httpClientConfig);

        //4.获取ES客户端连接
        JestClient jestClient = factory.getObject();





        //5.构建ES插入数据对象



        //6.执行插入数据操作
      //  jestClient.execute(index);

        //7.关闭连接
        jestClient.shutdownClient();

    }
}

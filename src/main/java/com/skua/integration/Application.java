package com.skua.integration;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.annotation.JsonAppend;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import net.minidev.json.JSONArray;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import com.rometools.rome.feed.synd.SyndEntry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.channel.MessageChannels;
import org.springframework.integration.dsl.core.Pollers;
import org.springframework.integration.dsl.file.Files;
import org.springframework.integration.dsl.mail.Mail;
import org.springframework.integration.dsl.support.Transformers;
import org.springframework.integration.feed.inbound.FeedEntryMessageSource;
import org.springframework.integration.file.support.FileExistsMode;
import org.springframework.integration.jdbc.JdbcMessageStore;
import org.springframework.integration.jdbc.JdbcPollingChannelAdapter;
import org.springframework.integration.jdbc.config.JdbcMessageStoreParser;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.messaging.handler.annotation.Payload;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;

import static org.springframework.core.SpringProperties.getProperty;

@SpringBootApplication
public class Application {


    @Autowired
    private DataSource dataSource;


    public String getSql(String where) {
        StringBuffer sqlB = new StringBuffer();
        sqlB.append("SELECT id,bookid FROM BOOK")
                .append(" ").append(where);
        return sqlB.toString();
    }

    ;


    public static void main(String[] args) {
        String str = "[{name:'a',value:'aa'},{name:'b',value:'bb'},{name:'c',value:'cc'},{name:'d',value:'dd'}]" ;

        JSONArray json = JSONArray.fromObject(str );



       // SpringApplication.run(Application.class, args);
    }

    //配置默认的轮询方式
    @Bean(name = PollerMetadata.DEFAULT_POLLER)
    public PollerMetadata poller() {
        return Pollers.fixedRate(50000).get();
    }


    @Bean
    public MessageSource<Object> jdbcMessageSource() {
        return new JdbcPollingChannelAdapter(this.dataSource, "SELECT id,bookid FROM BOOK where id > 1");
    }

 /*   @Bean
    public IntegrationFlow myFlow() throws IOException {
        return IntegrationFlows.from(jdbcMessageSource())
                //通过route来选择路由，消息体（payload）的类型为SyndEntry,判断条件类型为String
                //判断的值通过payload获得的分类（Categroy）
                //不同的分类值转向不同的消息通道
                .transform(Transformers.toJson())
                .channel("furtherProcessChannel")
                .<SyndEntry, String>route(payload -> payload.getCategories().get(0).getName(),
                        mapping -> mapping.channelMapping("id",
                                "idChannel"))
                .get();//获得IntegrationFlow实体，配置为Spring的Bean
    }
*/

    @Bean
    public IntegrationFlow pollingFlow() {
        return IntegrationFlows.from(jdbcMessageSource(),
                c -> c.poller(Pollers.fixedRate(10000).maxMessagesPerPoll(1)))
              //  .transform(Transformers.toJson())
                .channel("getMaxChannel")
                //.handle(message -> System.out.println(message.getPayload()))
                .get();
    }

    @Bean
    public IntegrationFlow transformFlow() {
        return IntegrationFlows.from("getMaxChannel")
                .transform(Transformers.toJson())
                .handle(message -> System.out.println(message.getPayload()))
                .get();
    }
/*    @Bean
    public IntegrationFlow furtherProcessFlow() {
        return IntegrationFlows.from("getMaxChannel")
                .transform(Transformers.toJson())
                .handle(message -> System.out.println("getMaxChannel" + message.getPayload()))
                .get();
    }*/
/*    @Bean
    public IntegrationFlow maxFlow() {
        return IntegrationFlows.from(MessageChannels.queue("getMaxChannel",
                2000))
                .transform(Transformers.toJson())
                .handle(message -> System.out.println("getMaxChannel" + message.getPayload()))
                .get();
    }*/
   /* @Bean
    public IntegrationFlow furtherProcessFlow() {
        return IntegrationFlows.from(MessageChannels.queue("furtherProcessChannel",
                3000))
                .transform(Transformers.toJson())
                .handle(message -> System.out.println("furtherProcessChannel" + message.getPayload()))
                .get();
    }*/

  /*  @Bean
    public IntegrationFlow releasesFlow() {
        return IntegrationFlows.from(MessageChannels.queue("idChannel",
                10))   //从releasesChannel获取数据
                .transform( //transform方法做数据转换，payload类型为SynEntry，将其转换为字符串类型，
                        // 并自定义数据的格式
                        // getProperty("line.separator") 换行符,功能和"\n"是一致的,但是此种写法屏蔽了 Windows和Linux的区别 ，更保险一些
                        payload -> "《" + payload.getTitle() + "》" +
                                payload.getLink() + getProperty("line.separator"))
                //handle方法处理file的出站适配器，Files类是由Spring Integration Java DSL提供的
                // Fluent API 用来构造文件输出的适配器
                .handle()
                .get();
    }*/

/*    @Bean
    public IntegrationFlow getFlow() {
        return IntegrationFlows.from(MessageChannels.queue("furtherProcessChannel",
                10))   //从releasesChannel获取数据
                //handle方法处理file的出站适配器，Files类是由Spring Integration Java DSL提供的
                // Fluent API 用来构造文件输出的适配器
                .handle(message -> System.out.println(message.getPayload()))
                .get();
    }*/


/*

    //自动获得资源
    @Value("https://spring.io/blog.atom")
    Resource resource;
    @Bean
    public FeedEntryMessageSource feedMessageSource() throws IOException {
        FeedEntryMessageSource messageSource = new
                FeedEntryMessageSource(resource.getURL(), "news");
        return messageSource;
    }
	@Bean
	public IntegrationFlow myFlow() throws IOException {
		return IntegrationFlows.from(feedMessageSource())
				//通过route来选择路由，消息体（payload）的类型为SyndEntry,判断条件类型为String
				//判断的值通过payload获得的分类（Categroy）
				//不同的分类值转向不同的消息通道
				.<SyndEntry, String>route(payload -> payload.getCategories().get(0).getName(),
						mapping -> mapping.channelMapping("releases",
								"releasesChannel")
								.channelMapping("engineering",
										"engineeringChannel")
								.channelMapping("news",
										"newsChannel"))
				.get();//获得IntegrationFlow实体，配置为Spring的Bean
	}


	//releases流程
	@Bean
	public IntegrationFlow releasesFlow() {
		return IntegrationFlows.from(MessageChannels.queue("releasesChannel",
				10))   //从releasesChannel获取数据
				.<SyndEntry, String>transform( //transform方法做数据转换，payload类型为SynEntry，将其转换为字符串类型，
						// 并自定义数据的格式
                        // getProperty("line.separator") 换行符,功能和"\n"是一致的,但是此种写法屏蔽了 Windows和Linux的区别 ，更保险一些
						payload -> "《" + payload.getTitle() + "》" +
								payload.getLink() + getProperty("line.separator"))
				//handle方法处理file的出站适配器，Files类是由Spring Integration Java DSL提供的
				// Fluent API 用来构造文件输出的适配器
				.handle(Files.outboundAdapter(new File("D:\\ares\\spring-integration-jdbc\\tmp\\springblog"))
						.fileExistsMode(FileExistsMode.APPEND)
						.charset("UTF-8")
						.fileNameGenerator(message -> "releases.txt")
						.get())
				.get();
	}

	//engineering流程，与releases流程相同
	@Bean
	public IntegrationFlow engineeringFlow() {
		return IntegrationFlows.from(MessageChannels.queue("engineeringChannel", 10))
				.<SyndEntry, String> transform(
						e -> "《" + e.getTitle() + "》" + e.getLink() +
								getProperty("line.separator"))
				.handle(Files.outboundAdapter(new File("D:\\ares\\spring-integration-jdbc\\tmp\\springblog"))
						.fileExistsMode(FileExistsMode.APPEND)
						.charset("UTF-8")
						.fileNameGenerator(message -> "engineering.txt")
						.get())
				.get();
	}
*/

/*	//news流程
	@Bean
	public IntegrationFlow newsFlow() {
		return IntegrationFlows.from(MessageChannels.queue("newsChannel", 10))
				.<SyndEntry, String> transform(
						payload -> "《" + payload.getTitle() + "》" +
								payload.getLink() + getProperty("line.separator"))
				//提供enrichHeaders方法增加消息头的信息
				.enrichHeaders(
						Mail.headers()
								.subject("来自Spring的新闻")
								.to("guang2_tan@126.com")
								.from("guang2_tan@126.com"))
				//邮件发送的相关信息通过Spring Integration Java DSL提供的Mail的headers方法来构造
				//使用handle方法自定义邮件发送的出站适配器，使用Spring Integration Java DSL提供的Mail.outboundAdapter来构造
				//使用guang2_tan@126.com向自己发送邮件
				.handle(Mail.outboundAdapter("smtp.126.com")
								.port(25)
								.protocol("smtp")
								.credentials("guang2_tan@126.com", "abc1234")
								.javaMailProperties(p -> p.put("mail.debug", "false")),
						e -> e.id("smtpOut"))
				.get();
	}*/
}

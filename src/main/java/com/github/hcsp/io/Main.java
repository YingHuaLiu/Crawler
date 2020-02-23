package com.github.hcsp.io;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.stream.Collectors;

public class Main {
    public static void main(String[] args) throws IOException, SQLException {
        //连接数据库
        Connection connection = DriverManager.getConnection("jdbc:h2:file:D:\\IdeaProjects\\Crawler\\news", "root", "123456");
        String link = null;

        while ((link = getNextLinkAndDelete(connection)) != null) {
            //从links_already_processed中查找是否存在link
            if (isLinkProcessed(connection, link)) {
                continue;
            }

            //如果link符合我们的要求，就进入
            if (isInterestingLink(link)) {
                //将html转换成可操作的document
                Document doc = httpGetAndParseHtml(link);
                //挑选出doc中的a标签
                ArrayList<Element> links = doc.select("a");
                //读取a标签中的href值，并将符合条件的href存入数据库
                //doc.select("a").stream().map(aTag -> aTag.attr("href"));
                for (Element aTag : links) {
                    String href = aTag.attr("href");
                    if (href.contains("news.sina.cn")) {
                        updateDataBase(connection, href, "insert into links_to_be_processed(link) values ?");
                    }
                }
                storeIntoDataBaseIfItIsNewsPage(connection, doc, link);
                //将link存入已处理表
                updateDataBase(connection, link, "insert into links_already_processed(link) values ?");
            }
        }
    }

    private static boolean isLinkProcessed(Connection connection, String link) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("select * from links_already_processed where link = ?")) {
            statement.setString(1, link);
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                return true;
            }
            return false;
        }
    }

    private static String getNextLink(Connection connection, String sql) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                return resultSet.getString(1);
            }
        }
        return null;
    }

    private static String getNextLinkAndDelete(Connection connection) throws SQLException {
        //从数据库中获取一个link
        String link = getNextLink(connection, "select link from links_to_be_processed limit 1");
        //如果link不为空，则从links_to_be_processed中删除link
        if (link != null) {
            updateDataBase(connection, link, "delete from links_to_be_processed where link = ?");
        }
        return link;

    }

    private static void updateDataBase(Connection connection, String link, String sql) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, link);
            statement.executeUpdate();
        }
    }

    private static void storeIntoDataBaseIfItIsNewsPage(Connection connection, Document doc, String link) throws SQLException {
        ArrayList<Element> articleTags = doc.select("article");
        if (!articleTags.isEmpty()) {
            for (Element articleTag : articleTags) {
                String title = articleTag.child(0).text();
                System.out.println(title);
                //获取p标签
                ArrayList<Element> paragraphs = articleTag.select("p");
                String content = paragraphs.stream().map(Element::text).collect(Collectors.joining("\n"));
                try (PreparedStatement statement = connection.prepareStatement("insert into news(url,title,content,created_at,modified_at) values (?,?,?,now(),now())")) {
                    statement.setString(1, link);
                    statement.setString(2, title);
                    statement.setString(3, content);
                    statement.executeUpdate();
                }
            }
        }
    }

    private static Document httpGetAndParseHtml(String link) throws IOException {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(link);
        httpGet.addHeader("User-Agent", "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Mobile Safari/537.36");
        System.out.println(link);
        try (CloseableHttpResponse response1 = httpclient.execute(httpGet)) {
            HttpEntity entity1 = response1.getEntity();
            String html = EntityUtils.toString(entity1);
            return Jsoup.parse(html);
        }
    }

    private static boolean isInterestingLink(String link) {
        return isLegalPage(link) && isNotLoginPage(link) &&
                (isNewsPage("news.sina.cn") || isIndexPage(link));
    }

    private static boolean isNotLoginPage(String link) {
        return !link.contains("passport.sina.cn");
    }

    private static boolean isLegalPage(String link) {
        return !link.contains("\\/");
    }

    private static boolean isIndexPage(String link) {
        return "https://sina.cn".equals(link);
    }

    private static boolean isNewsPage(String link) {
        return link.contains("news.sina.cn");
    }
}

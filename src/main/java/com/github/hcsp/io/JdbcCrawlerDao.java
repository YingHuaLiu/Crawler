package com.github.hcsp.io;

import java.sql.*;

public class JdbcCrawlerDao implements CrawlerDao{
    private final Connection connection;

    public JdbcCrawlerDao() {
        try {
            this.connection = DriverManager.getConnection("jdbc:h2:file:D:\\IdeaProjects\\Crawler\\news", "root", "123456");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean isLinkProcessed(String link) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("select * from links_already_processed where link = ?")) {
            statement.setString(1, link);
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                return true;
            }
            return false;
        }
    }

    public String getNextLink(String sql) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                return resultSet.getString(1);
            }
        }
        return null;
    }

    public String getNextLinkAndDelete() throws SQLException {
        //从数据库中获取一个link
        String link = getNextLink("select link from links_to_be_processed limit 1");
        //如果link不为空，则从links_to_be_processed中删除link
        if (link != null) {
            deleteLink(link, "delete from links_to_be_processed where link = ?");
        }
        return link;

    }

    public void deleteLink(String link, String sql) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, link);
            statement.executeUpdate();
        }
    }

    public void insertNewsIntoDatabase(String link, String title, String content) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("insert into news(url,title,content,created_at,modified_at) values (?,?,?,now(),now())")) {
            statement.setString(1, link);
            statement.setString(2, title);
            statement.setString(3, content);
            statement.executeUpdate();
        }
    }

    @Override
    public void insertPrcessedLink(String link) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("insert into links_already_processed(link) values ?")) {
            statement.setString(1, link);
            statement.executeUpdate();
        }
    }

    @Override
    public void insertLinkToBeProcessed(String href) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("insert into links_to_be_processed(link) values ?")) {
            statement.setString(1, href);
            statement.executeUpdate();
        }
    }
}

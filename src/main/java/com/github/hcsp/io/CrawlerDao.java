package com.github.hcsp.io;

import java.sql.SQLException;

public interface CrawlerDao {
    boolean isLinkProcessed(String link) throws SQLException;

    String getNextLink(String sql) throws SQLException;

    String getNextLinkAndDelete() throws SQLException;

    void updateDataBase(String link, String sql) throws SQLException;

    void insertNewsIntoDatabase(String link, String title, String content) throws SQLException;
}

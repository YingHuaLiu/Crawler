package com.github.hcsp.io;

import java.sql.SQLException;

public interface CrawlerDao {
    boolean isLinkProcessed(String link) throws SQLException;

    String getNextLinkAndDelete() throws SQLException;

    void insertNewsIntoDatabase(String link, String title, String content) throws SQLException;

    void insertPrcessedLink(String link) throws SQLException;

    void insertLinkToBeProcessed(String href) throws SQLException;
}

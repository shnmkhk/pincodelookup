package com.vishnu.controller;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

@RestController
public class TicksController {

	@Value("${spring.datasource.url}")
	private String dbUrl;
	
	@Value("${spring.datasource.username}")
	private String dbuser;
	
	@Value("${spring.datasource.password}")
	private String dbpwd;

	@Autowired
	private DataSource dataSource;

	@RequestMapping(path = "/createdb", produces = "application/json", method = RequestMethod.GET)
	public String createdb() {
		try (Connection connection = dataSource.getConnection()) {
			Statement stmt = connection.createStatement();
			stmt.executeUpdate("CREATE TABLE IF NOT EXISTS ticks (tick timestamp)");
			stmt.executeUpdate("INSERT INTO ticks VALUES (now())");

			return "{\"response\":\"success\"}";
		} catch (Exception e) {
			return "{\"response\":\"error\", \"details\":\"" + e.getMessage() + "\"}";
		}
	}

	@RequestMapping(path = "/fetchdata", produces = "application/json", method = RequestMethod.GET)
	public String fetchdata() {
		try (Connection connection = dataSource.getConnection()) {
			Statement stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT * FROM ticks");
			final StringBuilder sb = new StringBuilder();
			sb.append("{\"response\":[");
			boolean recordsFound = false;
			while (rs.next()) {
				sb.append("{\"tick\":\"" + rs.getTimestamp(1) + "\"},");
				recordsFound = true;
			}
			if (recordsFound) sb.setLength(sb.length() - 1);
			sb.append("]}");

			return sb.toString();
		} catch (Exception e) {
			return "{\"response\":\"error\", \"details\":\"" + e.getMessage() + "\"}";
		}
	}

	@Bean
	public DataSource dataSource() throws SQLException {
		System.out.println("dbUrl: " + dbUrl);
		System.out.println("unm: " + dbuser);
		System.out.println("pwd: " + dbpwd);
		if (dbUrl == null || dbUrl.isEmpty()) {
			return new HikariDataSource();
		} else {
			HikariConfig config = new HikariConfig();
			config.setJdbcUrl(dbUrl);
			config.setUsername(dbuser);
			config.setPassword(dbpwd);
			return new HikariDataSource(config);
		}
	}
}

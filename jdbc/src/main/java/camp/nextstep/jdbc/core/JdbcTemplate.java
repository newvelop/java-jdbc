package camp.nextstep.jdbc.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class JdbcTemplate {

    private static final Logger log = LoggerFactory.getLogger(JdbcTemplate.class);

    private final DataSource dataSource;

    public JdbcTemplate(final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void insert(String sql, Object... params) {
        try (Connection conn = getConnection()) {
            runUpdateQuery(conn, sql, params);
        } catch (SQLException e) {
            log.error("sql exception", e);
        }
    }

    public void update(String sql, Object... params) {
        try (Connection conn = getConnection()) {
            runUpdateQuery(conn, sql, params);
        } catch (SQLException e) {
            log.error("sql exception", e);
        }
    }

    public void delete(String sql, Object... params) {
        try (Connection conn = getConnection()) {
            runUpdateQuery(conn, sql, params);
        } catch (SQLException e) {
            log.error("sql exception", e);
        }
    }

    public <T> T queryForObject(final String sql, final RowMapper<T> rm, final Object... params) {
        final var list = query(sql, rm, params);
        return list == null || list.isEmpty() ? null : list.get(0);
    }

    public <T> List<T> query(final String sql, final RowMapper<T> rm, final Object... params) {
        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql);
         ) {
            List<T> result = new ArrayList<>();
            log.debug("query : {}", sql);
            defaultPreparedStatementSetter(params).setValues(pstmt);
            final var resultSet = pstmt.executeQuery();
            while (resultSet.next()) {
                try {
                    result.add(rm.mapRow(resultSet));
                } catch (SQLException e) {
                    log.error("sqlexception", e);
                    break;
                }
            }
            return result;
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private void runUpdateQuery(Connection conn, String sql, Object... params) {
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            log.debug("query : {}", sql);
            defaultPreparedStatementSetter(params).setValues(pstmt);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private Connection getConnection() {
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private PreparedStatementSetter defaultPreparedStatementSetter(Object... args) throws SQLException {
        return ps -> {
            try {
                for (int i = 0; i < args.length; i++) {
                    ps.setObject(i + 1, args[i]);
                }
            } catch (NullPointerException | SQLException e) {
                throw new SQLException(e);
            }
        };
    }
}

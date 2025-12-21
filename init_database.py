import mysql.connector
import os
from config import Config


def _connect_server():
    return mysql.connector.connect(
        host=Config.MYSQL_HOST,
        user=Config.MYSQL_USER,
        password=Config.MYSQL_PASSWORD,
        autocommit=True,
        connect_timeout=5,
        raise_on_warnings=False,
    )


def _connect_db():
    return mysql.connector.connect(
        host=Config.MYSQL_HOST,
        user=Config.MYSQL_USER,
        password=Config.MYSQL_PASSWORD,
        database=Config.MYSQL_DB,
        autocommit=False,
        connect_timeout=5,
        raise_on_warnings=False,
        charset="utf8mb4",
        use_unicode=True,
        buffered=True,
    )


def _exec_many(cursor, statements):
    for sql in statements:
        cursor.execute(sql)


def main():
    dbname = Config.MYSQL_DB
    server = None
    conn = None
    try:
        server = _connect_server()
        cur = server.cursor()
        cur.execute(
            f"CREATE DATABASE IF NOT EXISTS `{dbname}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
        )
        cur.close()
        server.close()
        conn = _connect_db()
        cursor = conn.cursor()

        users_sql = """
            CREATE TABLE IF NOT EXISTS users (
                user_id INT AUTO_INCREMENT PRIMARY KEY,
                email VARCHAR(255) NOT NULL UNIQUE,
                password_hash VARCHAR(255) NOT NULL,
                user_type ENUM('super_admin','hr','applicant') NOT NULL,
                is_active TINYINT(1) NOT NULL DEFAULT 1,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                last_login DATETIME NULL DEFAULT NULL,
                last_logout DATETIME NULL DEFAULT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """

        branches_sql = """
            CREATE TABLE IF NOT EXISTS branches (
                branch_id INT AUTO_INCREMENT PRIMARY KEY,
                branch_name VARCHAR(255) NOT NULL,
                address VARCHAR(255) NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """

        admins_sql = """
            CREATE TABLE IF NOT EXISTS admins (
                admin_id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT NOT NULL,
                full_name VARCHAR(255) NOT NULL,
                branch_id INT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                last_login DATETIME NULL DEFAULT NULL,
                last_logout DATETIME NULL DEFAULT NULL,
                INDEX idx_user_id (user_id),
                INDEX idx_branch_id (branch_id),
                CONSTRAINT fk_admin_user FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                CONSTRAINT fk_admin_branch FOREIGN KEY (branch_id) REFERENCES branches(branch_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """

        applicants_sql = """
            CREATE TABLE IF NOT EXISTS applicants (
                applicant_id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT NOT NULL,
                full_name VARCHAR(255) NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                last_login DATETIME NULL DEFAULT NULL,
                last_logout DATETIME NULL DEFAULT NULL,
                verification_token VARCHAR(255) NULL DEFAULT NULL,
                verification_token_expires DATETIME NULL DEFAULT NULL,
                last_profile_update DATETIME NULL DEFAULT NULL,
                INDEX idx_user_id (user_id),
                CONSTRAINT fk_applicant_user FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """

        positions_sql = """
            CREATE TABLE IF NOT EXISTS positions (
                position_id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(200) NOT NULL,
                department VARCHAR(200) NOT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """

        jobs_sql = """
            CREATE TABLE IF NOT EXISTS jobs (
                job_id INT AUTO_INCREMENT PRIMARY KEY,
                branch_id INT NULL,
                title VARCHAR(255) NOT NULL,
                description TEXT NULL,
                requirements TEXT NULL,
                status ENUM('open','closed') NOT NULL DEFAULT 'open',
                created_at DATETIME NULL DEFAULT NULL,
                posted_at DATETIME NULL DEFAULT NULL,
                posted_by INT NULL,
                allowed_extensions VARCHAR(255) NULL DEFAULT NULL,
                max_file_size_mb INT NULL DEFAULT NULL,
                required_file_types TEXT NULL DEFAULT NULL,
                location VARCHAR(255) NULL DEFAULT NULL,
                INDEX idx_branch_id (branch_id),
                INDEX idx_status (status),
                CONSTRAINT fk_jobs_branch FOREIGN KEY (branch_id) REFERENCES branches(branch_id),
                CONSTRAINT fk_jobs_posted_by FOREIGN KEY (posted_by) REFERENCES admins(admin_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """

        resumes_sql = """
            CREATE TABLE IF NOT EXISTS resumes (
                resume_id INT AUTO_INCREMENT PRIMARY KEY,
                applicant_id INT NOT NULL,
                file_name VARCHAR(255) NOT NULL,
                file_path VARCHAR(500) NOT NULL,
                file_type VARCHAR(50) NOT NULL DEFAULT 'resume',
                uploaded_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_applicant_id (applicant_id),
                CONSTRAINT fk_resume_applicant FOREIGN KEY (applicant_id) REFERENCES applicants(applicant_id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """

        applications_sql = """
            CREATE TABLE IF NOT EXISTS applications (
                application_id INT AUTO_INCREMENT PRIMARY KEY,
                applicant_id INT NOT NULL,
                job_id INT NOT NULL,
                status ENUM('pending','scheduled','interviewed','hired','rejected','withdrawn') NOT NULL DEFAULT 'pending',
                applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NULL DEFAULT NULL,
                viewed_at DATETIME NULL DEFAULT NULL,
                INDEX idx_applicant_id (applicant_id),
                INDEX idx_job_id (job_id),
                INDEX idx_status (status),
                CONSTRAINT fk_application_applicant FOREIGN KEY (applicant_id) REFERENCES applicants(applicant_id) ON DELETE CASCADE,
                CONSTRAINT fk_application_job FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """

        attachments_sql = """
            CREATE TABLE IF NOT EXISTS application_attachments (
                attachment_id INT AUTO_INCREMENT PRIMARY KEY,
                application_id INT NOT NULL,
                resume_id INT NOT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_application_id (application_id),
                INDEX idx_resume_id (resume_id),
                CONSTRAINT fk_attach_application FOREIGN KEY (application_id) REFERENCES applications(application_id) ON DELETE CASCADE,
                CONSTRAINT fk_attach_resume FOREIGN KEY (resume_id) REFERENCES resumes(resume_id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """

        interviews_sql = """
            CREATE TABLE IF NOT EXISTS interviews (
                interview_id INT AUTO_INCREMENT PRIMARY KEY,
                application_id INT NOT NULL,
                scheduled_date DATETIME NOT NULL,
                location VARCHAR(255) NULL,
                status ENUM('scheduled','confirmed','rescheduled','completed','cancelled','no_show') DEFAULT 'scheduled',
                interview_mode VARCHAR(50) NULL DEFAULT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_application_id (application_id),
                INDEX idx_status (status),
                CONSTRAINT fk_interview_application FOREIGN KEY (application_id) REFERENCES applications(application_id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """

        notifications_sql = """
            CREATE TABLE IF NOT EXISTS notifications (
                notification_id INT AUTO_INCREMENT PRIMARY KEY,
                application_id INT NULL,
                message TEXT NOT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                sent_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                is_read TINYINT(1) NOT NULL DEFAULT 0,
                INDEX idx_application_id (application_id),
                INDEX idx_is_read (is_read),
                CONSTRAINT fk_notification_application FOREIGN KEY (application_id) REFERENCES applications(application_id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """

        auth_sessions_sql = """
            CREATE TABLE IF NOT EXISTS auth_sessions (
                session_id VARCHAR(64) PRIMARY KEY,
                user_id INT NOT NULL,
                session_data TEXT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                expires_at DATETIME NOT NULL,
                is_active TINYINT(1) NULL DEFAULT 1,
                last_activity DATETIME NULL DEFAULT NULL,
                logout_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_user_id (user_id),
                CONSTRAINT fk_auth_user FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """

        activity_logs_sql = """
            CREATE TABLE IF NOT EXISTS activity_logs (
                log_id INT AUTO_INCREMENT PRIMARY KEY,
                admin_id INT,
                action VARCHAR(255) NOT NULL,
                target_table VARCHAR(255) NOT NULL,
                target_id INT,
                details TEXT,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_admin_id (admin_id),
                INDEX idx_created_at (created_at),
                INDEX idx_target (target_table, target_id),
                CONSTRAINT fk_activity_admin FOREIGN KEY (admin_id) REFERENCES admins(admin_id) ON DELETE SET NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """

        activity_deletions_sql = """
            CREATE TABLE IF NOT EXISTS activity_log_deletions (
                id INT AUTO_INCREMENT PRIMARY KEY,
                branch_id INT NULL,
                deleted_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_branch_id (branch_id),
                INDEX idx_deleted_at (deleted_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """

        _exec_many(
            cursor,
            [
                users_sql,
                branches_sql,
                admins_sql,
                applicants_sql,
                positions_sql,
                jobs_sql,
                resumes_sql,
                applications_sql,
                attachments_sql,
                interviews_sql,
                notifications_sql,
                auth_sessions_sql,
                activity_logs_sql,
                activity_deletions_sql,
            ],
        )

        conn.commit()
        cursor.close()
        conn.close()
        print("OK")
    except Exception as e:
        try:
            if conn:
                conn.rollback()
        except Exception:
            pass
        print(f"ERROR: {e}")
        raise


if __name__ == "__main__":
    main()


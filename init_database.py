#!/usr/bin/env python3
"""
Production Database Initialization Script for Recruitment System
Simplified version for reynald user
"""

import mysql.connector
from mysql.connector import Error
import os
import sys
import argparse
from dotenv import load_dotenv
import bcrypt
import logging
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('database_init.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DatabaseInitializer:
    def __init__(self, env_file='.env.production'):
        """Initialize with environment"""
        # Load environment variables
        load_dotenv(env_file)
        
        # Database configuration
        self.host = os.getenv('DB_HOST', 'localhost')
        self.user = os.getenv('DB_USER', 'reynald')
        self.password = os.getenv('DB_PASSWORD', 'Abacial@2004')
        self.database = os.getenv('DB_NAME', 'recruitment_system')
        self.port = os.getenv('DB_PORT', '3306')
        
        # Admin credentials
        self.admin_email = os.getenv('ADMIN_EMAIL', 'laicabarey@gmail.com')
        self.admin_password = os.getenv('ADMIN_PASSWORD', 'whitehat88@2026')
        
        logger.info(f"Using database: {self.database}")
        logger.info(f"Using MySQL user: {self.user}")
    
    def create_connection(self, use_database=True):
        """Create database connection"""
        try:
            config = {
                'host': self.host,
                'user': self.user,
                'password': self.password,
                'port': int(self.port),
                'connection_timeout': 30
            }
            
            if use_database:
                config['database'] = self.database
                
            connection = mysql.connector.connect(**config)
            return connection
        except Error as e:
            logger.error(f"Connection error: {e}")
            raise
    
    def hash_password(self, password):
        """Hash a password using bcrypt"""
        salt = bcrypt.gensalt(rounds=12)
        return bcrypt.hashpw(password.encode('utf-8'), salt).decode('utf-8')
    
    def drop_all_tables(self):
        """Drop all existing tables"""
        try:
            connection = self.create_connection()
            cursor = connection.cursor()
            
            # Disable foreign key checks
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
            
            # Get all table names
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            
            if tables:
                logger.info(f"Dropping {len(tables)} tables...")
                for table in tables:
                    table_name = table[0]
                    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                    logger.debug(f"Dropped: {table_name}")
            
            # Re-enable foreign key checks
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            
            connection.commit()
            logger.info("All tables dropped")
            return True
            
        except Error as e:
            logger.error(f"Error dropping tables: {e}")
            return False
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'connection' in locals():
                connection.close()
    
    def create_tables(self):
        """Create all database tables"""
        try:
            connection = self.create_connection()
            cursor = connection.cursor()
            
            logger.info("Creating tables...")
            
            # Table: users
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INT(20) NOT NULL AUTO_INCREMENT,
                email VARCHAR(100) NOT NULL,
                password_hash VARCHAR(255) NOT NULL,
                user_type ENUM('super_admin', 'hr', 'applicant') DEFAULT 'applicant',
                is_active TINYINT(1) DEFAULT 1,
                email_verified TINYINT(1) DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                last_login DATETIME DEFAULT NULL,
                last_logout DATETIME DEFAULT NULL,
                PRIMARY KEY (user_id),
                UNIQUE KEY (email)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            
            # Table: branches
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS branches (
                branch_id INT(20) NOT NULL AUTO_INCREMENT,
                branch_name VARCHAR(100) NOT NULL,
                address VARCHAR(255) NOT NULL,
                operating_hours VARCHAR(100) DEFAULT NULL,
                is_active TINYINT(1) DEFAULT 1,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (branch_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            
            # Table: admins
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS admins (
                admin_id INT(20) NOT NULL AUTO_INCREMENT,
                user_id INT(20) NOT NULL,
                full_name VARCHAR(100) NOT NULL,
                email VARCHAR(100) NOT NULL,
                password_hash VARCHAR(255) DEFAULT NULL,
                role VARCHAR(20) DEFAULT 'hr',
                is_active TINYINT(1) DEFAULT 1,
                branch_id INT(20) DEFAULT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                last_login DATETIME DEFAULT NULL,
                last_logout DATETIME DEFAULT NULL,
                PRIMARY KEY (admin_id),
                UNIQUE KEY (email),
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                FOREIGN KEY (branch_id) REFERENCES branches(branch_id) ON DELETE SET NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            
            # Table: applicants
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS applicants (
                applicant_id INT(20) NOT NULL AUTO_INCREMENT,
                user_id INT(20) NOT NULL,
                full_name VARCHAR(100) NOT NULL,
                email VARCHAR(100) NOT NULL,
                phone_number VARCHAR(20) DEFAULT NULL,
                password_hash VARCHAR(255) DEFAULT NULL,
                verification_token VARCHAR(255) DEFAULT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                last_login DATETIME DEFAULT NULL,
                last_logout DATETIME DEFAULT NULL,
                verification_token_expires DATETIME DEFAULT NULL,
                last_profile_update DATETIME DEFAULT NULL,
                PRIMARY KEY (applicant_id),
                UNIQUE KEY (email),
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            
            # Table: jobs
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                job_id INT(20) NOT NULL AUTO_INCREMENT,
                job_title VARCHAR(100) NOT NULL,
                job_description LONGTEXT NOT NULL,
                job_requirements LONGTEXT DEFAULT NULL,
                location VARCHAR(100) DEFAULT NULL,
                branch_id INT(20) DEFAULT NULL,
                posted_by INT(20) NOT NULL,
                status ENUM('open', 'closed') DEFAULT 'open',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (job_id),
                FOREIGN KEY (branch_id) REFERENCES branches(branch_id) ON DELETE SET NULL,
                FOREIGN KEY (posted_by) REFERENCES admins(admin_id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            
            # Table: resumes
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS resumes (
                resume_id INT(20) NOT NULL AUTO_INCREMENT,
                applicant_id INT(20) NOT NULL,
                file_name VARCHAR(255) NOT NULL,
                file_path VARCHAR(255) NOT NULL,
                uploaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                file_type VARCHAR(50) DEFAULT 'resume',
                PRIMARY KEY (resume_id),
                FOREIGN KEY (applicant_id) REFERENCES applicants(applicant_id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            
            # Table: applications
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS applications (
                application_id INT(20) NOT NULL AUTO_INCREMENT,
                job_id INT(20) NOT NULL,
                resume_id INT(20) DEFAULT NULL,
                applicant_id INT(20) NOT NULL,
                status ENUM('pending', 'scheduled', 'interviewed', 'hired', 'rejected', 'withdrawn') DEFAULT 'pending',
                archived_at DATETIME DEFAULT NULL,
                applied_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                viewed_at DATETIME DEFAULT NULL,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (application_id),
                FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE,
                FOREIGN KEY (applicant_id) REFERENCES applicants(applicant_id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            
            # Table: application_attachments
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS application_attachments (
                attachment_id INT(11) NOT NULL AUTO_INCREMENT,
                application_id INT(11) NOT NULL,
                resume_id INT(11) NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (attachment_id),
                FOREIGN KEY (application_id) REFERENCES applications(application_id),
                FOREIGN KEY (resume_id) REFERENCES resumes(resume_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            
            # Table: interviews
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS interviews (
                interview_id INT(20) NOT NULL AUTO_INCREMENT,
                application_id INT(20) NOT NULL,
                scheduled_date DATETIME NOT NULL,
                interview_type VARCHAR(50) DEFAULT NULL,
                status ENUM('scheduled', 'confirmed', 'rescheduled', 'completed', 'cancelled', 'no_show') DEFAULT 'scheduled',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                interview_mode VARCHAR(50) DEFAULT NULL,
                PRIMARY KEY (interview_id),
                FOREIGN KEY (application_id) REFERENCES applications(application_id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            
            # Table: saved_jobs
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS saved_jobs (
                saved_job_id INT(20) NOT NULL AUTO_INCREMENT,
                applicant_id INT(20) NOT NULL,
                job_id INT(20) NOT NULL,
                saved_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (saved_job_id),
                FOREIGN KEY (applicant_id) REFERENCES applicants(applicant_id) ON DELETE CASCADE,
                FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            
            # Table: notifications
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS notifications (
                notification_id INT(20) NOT NULL AUTO_INCREMENT,
                applicant_id INT(20) DEFAULT NULL,
                admin_id INT(20) DEFAULT NULL,
                application_id INT(20) DEFAULT NULL,
                message VARCHAR(255) NOT NULL,
                is_read TINYINT(1) DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                sent_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (notification_id),
                FOREIGN KEY (applicant_id) REFERENCES applicants(applicant_id) ON DELETE CASCADE,
                FOREIGN KEY (admin_id) REFERENCES admins(admin_id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            
            # Table: auth_sessions
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS auth_sessions (
                session_id VARCHAR(255) NOT NULL,
                user_id INT(20) NOT NULL,
                session_data LONGTEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                expires_at DATETIME NOT NULL,
                logout_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                is_active TINYINT(1) DEFAULT 1,
                PRIMARY KEY (session_id),
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            
            # Table: password_resets
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS password_resets (
                id INT(20) NOT NULL AUTO_INCREMENT,
                user_email VARCHAR(100) NOT NULL,
                token VARCHAR(255) NOT NULL,
                role ENUM('admin', 'applicant', 'hr') NOT NULL,
                expired_at DATETIME NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            
            # Table: admin_2fa_verification
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS admin_2fa_verification (
                id INT(20) NOT NULL AUTO_INCREMENT,
                user_id INT(20) NOT NULL,
                email VARCHAR(255) NOT NULL,
                verification_code VARCHAR(6) NOT NULL,
                temp_token VARCHAR(255) NOT NULL,
                attempts INT(3) DEFAULT 0,
                max_attempts INT(3) DEFAULT 3,
                verified TINYINT(1) DEFAULT 0,
                expired_at DATETIME NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (id),
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            
            # Table: hr_2fa_verification
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS hr_2fa_verification (
                id INT(20) NOT NULL AUTO_INCREMENT,
                user_id INT(20) NOT NULL,
                email VARCHAR(255) NOT NULL,
                verification_code VARCHAR(6) NOT NULL,
                temp_token VARCHAR(255) NOT NULL,
                attempts INT(3) DEFAULT 0,
                max_attempts INT(3) DEFAULT 3,
                verified TINYINT(1) DEFAULT 0,
                expired_at DATETIME NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (id),
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            
            # Table: activity_logs
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS activity_logs (
                log_id INT(20) NOT NULL AUTO_INCREMENT,
                user_id INT(20) DEFAULT NULL,
                action VARCHAR(100) NOT NULL,
                target_table VARCHAR(255) DEFAULT '',
                target_id INT(11) DEFAULT NULL,
                description LONGTEXT DEFAULT NULL,
                logged_at DATETIME DEFAULT CURRENT_TIMESTAMP(),
                PRIMARY KEY (log_id),
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            
            # Table: profile_changes
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS profile_changes (
                id INT(20) NOT NULL AUTO_INCREMENT,
                user_id INT(20) DEFAULT NULL,
                admin_id INT(20) DEFAULT NULL,
                applicant_id INT(20) DEFAULT NULL,
                change_type VARCHAR(50) DEFAULT NULL,
                old_value LONGTEXT DEFAULT NULL,
                new_value LONGTEXT DEFAULT NULL,
                changed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (id),
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                FOREIGN KEY (admin_id) REFERENCES admins(admin_id) ON DELETE CASCADE,
                FOREIGN KEY (applicant_id) REFERENCES applicants(applicant_id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            
            connection.commit()
            logger.info("All tables created successfully")
            return True
            
        except Error as e:
            logger.error(f"Error creating tables: {e}")
            return False
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'connection' in locals():
                connection.close()
    
    def insert_initial_data(self):
        """Insert initial data - ONLY ONE ADMIN ACCOUNT"""
        try:
            connection = self.create_connection()
            cursor = connection.cursor()
            
            # Hash password
            password = self.hash_password(self.admin_password)
            
            logger.info("Inserting initial data...")
            
            # Insert ONLY ONE super admin user - laicabarey@gmail.com
            cursor.execute("""
            INSERT IGNORE INTO users (email, password_hash, user_type, is_active, email_verified, created_at, last_login)
            VALUES (%s, %s, 'super_admin', 1, 1, NOW(), NOW())
            """, (self.admin_email, password))
            
            # Get the inserted user ID
            user_id = cursor.lastrowid
            if user_id == 0:  # User already exists
                cursor.execute("SELECT user_id FROM users WHERE email = %s", (self.admin_email,))
                user_id = cursor.fetchone()[0]
            
            # Insert admin record with NULL branch_id
            cursor.execute("""
            INSERT IGNORE INTO admins (user_id, full_name, email, password_hash, role, is_active, branch_id, created_at, last_login)
            VALUES (%s, %s, %s, %s, 'admin', 1, NULL, NOW(), NOW())
            """, (user_id, "System Administrator", self.admin_email, password))
            
            connection.commit()
            logger.info("Initial data inserted successfully")
            
            return True
            
        except Error as e:
            logger.error(f"Error inserting initial data: {e}")
            return False
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'connection' in locals():
                connection.close()
    
    def verify_setup(self):
        """Verify the database setup"""
        try:
            connection = self.create_connection()
            cursor = connection.cursor()
            
            # Check tables
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            logger.info(f"Found {len(tables)} tables in database")
            
            # Check admin user
            cursor.execute("SELECT email, user_type FROM users WHERE email = %s", (self.admin_email,))
            admin = cursor.fetchone()
            
            if admin:
                logger.info(f"✓ Admin account: {admin[0]} ({admin[1]})")
            else:
                logger.error("✗ Admin account not found")
                return False
            
            cursor.close()
            connection.close()
            return True
            
        except Error as e:
            logger.error(f"Verification failed: {e}")
            return False
    
    def initialize(self, force=False):
        """Initialize database"""
        start_time = time.time()
        
        print("\n" + "="*60)
        print("RECRUITMENT SYSTEM DATABASE INITIALIZATION")
        print("="*60)
        print(f"Database: {self.database}")
        print(f"MySQL User: {self.user}")
        print(f"Admin: {self.admin_email}")
        print("="*60)
        
        if not force:
            print("\n⚠️  WARNING: This will DROP ALL EXISTING TABLES!")
            confirm = input("Type 'YES' to continue: ").strip().upper()
            if confirm != 'YES':
                print("Operation cancelled")
                return
        
        try:
            logger.info("Starting database initialization...")
            
            # Step 1: Drop tables
            logger.info("\n[1/3] Dropping existing tables...")
            if not self.drop_all_tables():
                logger.error("Failed to drop tables")
                return False
            
            # Step 2: Create tables
            logger.info("\n[2/3] Creating tables...")
            if not self.create_tables():
                logger.error("Failed to create tables")
                return False
            
            # Step 3: Insert data
            logger.info("\n[3/3] Inserting initial data...")
            if not self.insert_initial_data():
                logger.error("Failed to insert data")
                return False
            
            # Verify
            logger.info("\n[Verification] Verifying setup...")
            if not self.verify_setup():
                logger.error("Verification failed")
                return False
            
            elapsed_time = time.time() - start_time
            
            print("\n" + "="*60)
            print("✅ DATABASE INITIALIZATION COMPLETE")
            print("="*60)
            print(f"Time: {elapsed_time:.2f} seconds")
            print(f"Database: {self.database}")
            print(f"MySQL User: {self.user}")
            print(f"Admin Email: {self.admin_email}")
            print(f"Admin Password: {self.admin_password}")
            print("\nNEXT STEPS:")
            print("1. Start your application")
            print("2. Login with admin credentials")
            print("3. Change password for security")
            print("="*60)
            
            return True
            
        except Exception as e:
            logger.error(f"Initialization failed: {e}", exc_info=True)
            return False

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Initialize recruitment system database')
    parser.add_argument('--force', action='store_true', help='Skip confirmation')
    parser.add_argument('--env', default='.env.production', help='Environment file')
    
    args = parser.parse_args()
    
    try:
        initializer = DatabaseInitializer(env_file=args.env)
        success = initializer.initialize(force=args.force)
        
        if success:
            sys.exit(0)
        else:
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\nOperation cancelled")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

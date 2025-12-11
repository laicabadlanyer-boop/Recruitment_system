import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv
import bcrypt
import uuid
from datetime import datetime, timedelta

# Load environment variables
load_dotenv()

class DatabaseInitializer:
    def __init__(self):
        self.host = os.getenv('DB_HOST', 'localhost')
        self.user = os.getenv('DB_USER', 'root')
        self.password = os.getenv('DB_PASSWORD', '')
        self.database = os.getenv('DB_NAME', 'recruitment_system')
        
    def create_connection(self):
        """Create a database connection"""
        try:
            connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password
            )
            return connection
        except Error as e:
            print(f"Error connecting to MySQL: {e}")
            return None
    
    def create_database(self):
        """Create the database if it doesn't exist"""
        connection = self.create_connection()
        if connection:
            cursor = connection.cursor()
            try:
                # Create database
                cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
                print(f"✓ Database '{self.database}' created or already exists")
                
                # Use the database
                cursor.execute(f"USE {self.database}")
                
                connection.commit()
                cursor.close()
                connection.close()
                return True
            except Error as e:
                print(f"✗ Error creating database: {e}")
                return False
        else:
            print("✗ Failed to connect to MySQL server")
            return False
    
    def hash_password(self, password):
        """Hash a password using bcrypt"""
        salt = bcrypt.gensalt(rounds=12)
        return bcrypt.hashpw(password.encode('utf-8'), salt).decode('utf-8')
    
    def drop_all_tables(self):
        """Drop all existing tables"""
        try:
            connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            cursor = connection.cursor()
            
            # Disable foreign key checks temporarily
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
            
            # Get all table names
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            
            if tables:
                print(f"Found {len(tables)} tables to drop")
                # Drop all tables
                for table in tables:
                    table_name = table[0]
                    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                    print(f"  ✓ Dropped table: {table_name}")
            else:
                print("  No existing tables found")
            
            # Re-enable foreign key checks
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            
            connection.commit()
            print("✓ All tables dropped successfully")
            
        except Error as e:
            print(f"✗ Error dropping tables: {e}")
            return False
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'connection' in locals():
                connection.close()
        return True
    
    def create_tables(self):
        """Create all database tables"""
        try:
            connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            cursor = connection.cursor()
            
            print("Creating tables...")
            
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
            print("  ✓ Created table: users")
            
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
            print("  ✓ Created table: branches")
            
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
            print("  ✓ Created table: admins")
            
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
            print("  ✓ Created table: applicants")
            
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
            print("  ✓ Created table: jobs")
            
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
            print("  ✓ Created table: resumes")
            
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
            print("  ✓ Created table: applications")
            
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
            print("  ✓ Created table: application_attachments")
            
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
            print("  ✓ Created table: interviews")
            
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
            print("  ✓ Created table: saved_jobs")
            
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
            print("  ✓ Created table: notifications")
            
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
            print("  ✓ Created table: auth_sessions")
            
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
            print("  ✓ Created table: password_resets")
            
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
            print("  ✓ Created table: admin_2fa_verification")
            
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
            print("  ✓ Created table: hr_2fa_verification")
            
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
            print("  ✓ Created table: activity_logs")
            
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
            print("  ✓ Created table: profile_changes")
            
            connection.commit()
            print("✓ All tables created successfully!")
            
            return True
            
        except Error as e:
            print(f"✗ Error creating tables: {e}")
            if 'connection' in locals():
                connection.rollback()
            return False
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'connection' in locals():
                connection.close()
    
    def insert_initial_data(self):
        """Insert initial data into the database - ONLY ONE ADMIN ACCOUNT"""
        try:
            connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            cursor = connection.cursor()
            
            # Hash password for the single admin account
            password = self.hash_password("whitehat88@2026")
            
            print("Inserting initial data...")
            
            # Insert ONLY ONE super admin user - laicabarey@gmail.com
            cursor.execute("""
            INSERT INTO users (email, password_hash, user_type, is_active, email_verified, created_at, last_login)
            VALUES (%s, %s, 'super_admin', 1, 1, NOW(), NOW())
            """, ("laicabarey@gmail.com", password))
            
            # Get the inserted user ID
            user_id = cursor.lastrowid
            
            # Insert admin record
            cursor.execute("""
            INSERT INTO admins (user_id, full_name, email, password_hash, role, is_active, created_at, last_login)
            VALUES (%s, %s, %s, %s, 'admin', 1, NOW(), NOW())
            """, (user_id, "System Administrator", "laicabarey@gmail.com", password))
            
            # Insert a sample branch
            cursor.execute("""
            INSERT INTO branches (branch_name, address, operating_hours, is_active)
            VALUES (%s, %s, %s, 1)
            """, ("Main Branch", "123 Main Street, City Center", "Mon-Fri: 9AM-5PM"))
            
            connection.commit()
            print("✓ Initial data inserted successfully!")
            
            return True
            
        except Error as e:
            print(f"✗ Error inserting initial data: {e}")
            if 'connection' in locals():
                connection.rollback()
            return False
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'connection' in locals():
                connection.close()
    
    def initialize_database(self):
        """Main method to initialize the database"""
        print("\n" + "="*60)
        print("RECRUITMENT SYSTEM DATABASE INITIALIZATION")
        print("="*60)
        print("WARNING: This will DESTROY ALL EXISTING DATA!")
        print("\nThis script will:")
        print("  1. Drop ALL existing tables")
        print("  2. Create ALL tables from scratch")
        print("  3. Create ONLY ONE admin account")
        print("\nREMOVED TABLES:")
        print("  - applicant_2fa_verification")
        print("  - hr_accounts")
        print("  - positions")
        print("  - schema_migrations")
        print("\nAdmin account credentials:")
        print("  Email: laicabarey@gmail.com")
        print("  Password: whitehat88@2026")
        print("\nIMPORTANT: NO admin@whitehat88.com account will be created!")
        print("="*60)
        
        # Confirm with user
        while True:
            confirm = input("\nType 'YES' to proceed or 'NO' to cancel: ").strip().upper()
            if confirm == 'YES':
                break
            elif confirm == 'NO':
                print("\nOperation cancelled by user.")
                return
            else:
                print("Please type 'YES' or 'NO'")
        
        print("\nStarting database initialization...")
        
        # Create database if not exists
        print("\n[1/4] Checking/Creating database...")
        if not self.create_database():
            print("✗ Failed to create database. Exiting...")
            return
        
        # Drop all existing tables
        print("\n[2/4] Dropping existing tables...")
        if not self.drop_all_tables():
            print("✗ Failed to drop tables. Exiting...")
            return
        
        # Create all tables
        print("\n[3/4] Creating tables...")
        if not self.create_tables():
            print("✗ Failed to create tables. Exiting...")
            return
        
        # Insert initial data (only one admin account)
        print("\n[4/4] Inserting initial data...")
        if not self.insert_initial_data():
            print("✗ Failed to insert initial data. Exiting...")
            return
        
        print("\n" + "="*60)
        print("✓ DATABASE INITIALIZATION COMPLETED SUCCESSFULLY!")
        print("="*60)
        print("\nSUCCESS SUMMARY:")
        print("  ✓ Database created/verified")
        print("  ✓ All old tables removed")
        print("  ✓ All new tables created")
        print("  ✓ ONLY ONE admin account created")
        print("\nREMOVED TABLES:")
        print("  - applicant_2fa_verification")
        print("  - hr_accounts")
        print("  - positions")
        print("  - schema_migrations")
        print("\nADMIN LOGIN DETAILS:")
        print("  Email:    laicabarey@gmail.com")
        print("  Password: whitehat88@2026")
        print("\nIMPORTANT:")
        print("  - Only ONE admin account created")
        print("  - NO admin@whitehat88.com account created")
        print("  - NO other accounts created")
        print("\nNEXT STEPS:")
        print("  1. Start your application")
        print("  2. Login with the credentials above")
        print("  3. Change your password for security")
        print("="*60)

def main():
    # Create an instance of the initializer
    initializer = DatabaseInitializer()
    
    try:
        # Initialize the database
        initializer.initialize_database()
    except KeyboardInterrupt:
        print("\n\nOperation interrupted by user. Exiting...")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")

if __name__ == "__main__":
    main()
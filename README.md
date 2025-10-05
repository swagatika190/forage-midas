💰 Midas - Real-Time User Data Processing System
A real-time data processing application built as part of the J.P. Morgan Chase & Co. Advanced Software Engineering Virtual Experience Program on Forage. This project implements event-driven architecture using Apache Kafka for streaming user data with balance querying and file processing capabilities.

Show Image
Show Image
Show Image
Show Image
Show Image

📋 Table of Contents
Overview
Features
Tech Stack
System Architecture
Kafka Integration
Database Schema
Installation & Setup
Usage
Project Structure
What I Learned
About JPMC Forage Program
Contact
🎯 Overview
Midas is a real-time data processing system developed as part of J.P. Morgan's Advanced Software Engineering virtual experience program. The application demonstrates enterprise-level event-driven architecture using Apache Kafka for streaming user records, with capabilities for balance querying, file processing, and data population.

Project Context
This project was completed as part of the J.P. Morgan Chase & Co. virtual experience program on Forage, simulating real-world software engineering tasks performed at JPMC.

Key Objectives
Implement event-driven architecture with Apache Kafka
Build real-time data streaming pipelines
Process and store user records efficiently
Query user balances in real-time
Handle large-scale data file processing
Use Cases
Real-Time User Data Streaming - Stream user records through Kafka topics
Balance Management - Query user account balances instantly
Bulk Data Processing - Load and process user data from files
Event-Driven Updates - React to user record changes in real-time
Data Population - Populate system with initial user datasets
✨ Features
Kafka Streaming
📨 Message Production - Publish user record events to Kafka topics
📥 Message Consumption - Subscribe to and process Kafka messages in real-time
🔄 Event-Driven Processing - React to user record changes asynchronously
📊 Topic Management - Organize data streams by topic
⚡ High Throughput - Process thousands of messages per second
🔁 Message Replay - Reprocess historical data when needed
User Management
👤 User Records - Store and manage comprehensive user information
💰 Balance Tracking - Track and query user account balances
🔍 Balance Querying - Efficient balance lookup operations
📝 Record Updates - Update user information in real-time
🗂️ Data Persistence - Reliable storage of user records
Data Processing
📂 File Loading - Load user data from external files (CSV, JSON, etc.)
🔄 Bulk Population - Populate database with large datasets efficiently
⚙️ Data Transformation - Transform data between formats
✅ Data Validation - Validate user records before processing
📊 Batch Processing - Handle large volumes of data efficiently
Application Features
🏗️ Component-Based Architecture - Modular, reusable components
⚙️ Configuration Management - Externalized configuration for flexibility
🗄️ Repository Pattern - Clean data access abstraction
🎯 Foundation Layer - Core utilities and base classes
🔒 Error Handling - Robust exception management
📝 Logging - Comprehensive application logging
🛠️ Tech Stack
Backend Framework
Java 8+ - Core programming language
Spring Boot - Application framework
Spring Data JPA - Database access and ORM
Hibernate - Object-Relational Mapping
Maven - Build and dependency management
Message Streaming
Apache Kafka - Distributed event streaming platform
Producer API for publishing messages
Consumer API for subscribing to topics
High-throughput, low-latency messaging
Fault-tolerant message storage
Scalable distributed architecture
Database
MySQL - Relational database for user records
User data persistence
Balance information storage
Transaction support
ACID compliance
Architecture Patterns
Event-Driven Architecture - Kafka-based messaging
Repository Pattern - Data access abstraction
Component-Based Design - Modular architecture
Producer-Consumer Pattern - Kafka integration
Layered Architecture - Separation of concerns
Tools
Git & GitHub - Version control
Postman - API testing
MySQL Workbench - Database management
IntelliJ IDEA / Eclipse - IDE
Kafka Tools - Kafka cluster management and monitoring
🏗️ System Architecture
High-Level Architecture
┌─────────────────────────────────────────────────────────┐
│              External Data Sources                       │
│         (Files: CSV, JSON, External APIs)               │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│                File Loader Component                     │
│  - Read files from filesystem                           │
│  - Parse CSV/JSON data                                  │
│  - Validate data format                                 │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│              User Populator Component                    │
│  - Transform file data to UserRecord entities           │
│  - Bulk insert to database                              │
│  - Publish to Kafka topics                              │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│                Kafka Producer                            │
│  - Serialize UserRecord objects                         │
│  - Publish to 'user-records' topic                      │
│  - Handle acknowledgments                               │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
        ┌────────────────────────────┐
        │     Apache Kafka Cluster    │
        ├────────────────────────────┤
        │  Topics:                   │
        │  - user-records            │
        │  - user-updates            │
        │  - balance-queries         │
        │                            │
        │  Partitions: 3             │
        │  Replication Factor: 1     │
        └────────────┬───────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│                Kafka Consumer                            │
│  - Subscribe to topics                                  │
│  - Deserialize messages                                 │
│  - Process user records                                 │
│  - Commit offsets                                       │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│             Midas Core Application                       │
│  - Business logic processing                            │
│  - Balance calculations                                 │
│  - Data validation                                      │
│  - Error handling                                       │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│              Repository Layer                            │
│  (UserRecordRepository)                                 │
│  - CRUD operations                                      │
│  - Custom queries                                       │
│  - Balance queries                                      │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│              Database (MySQL)                            │
│  - User records storage                                 │
│  - Balance information                                  │
│  - Transaction logs                                     │
└─────────────────────────────────────────────────────────┘
🔥 Kafka Integration
Kafka Architecture Overview
┌──────────────────┐         ┌─────────────────┐         ┌──────────────────┐
│  Kafka Producer  │────────>│  Kafka Broker   │────────>│  Kafka Consumer  │
│  (KafkaProducer  │ Publish │  (Topic)        │ Poll    │  (KafkaConsumer  │
│   .java)         │ Message │  - Partitions   │ Message │   .java)         │
└──────────────────┘         └─────────────────┘         └──────────────────┘
                                      │
                                      │ Persist
                                      ▼
                             ┌─────────────────┐
                             │   Kafka Logs    │
                             │  (Disk Storage) │
                             └─────────────────┘
Kafka Topics Configuration
User Records Topic:

properties
Topic Name: user-records
Partitions: 3
Replication Factor: 1
Retention: 7 days
Compression: gzip
Message Format:

json
{
  "userId": 12345,
  "username": "john_doe",
  "email": "john@example.com",
  "balance": 1500.50,
  "accountStatus": "ACTIVE",
  "timestamp": "2024-11-15T10:30:00Z"
}
Producer Configuration
java
// KafkaProducer.java
- Bootstrap Servers: localhost:9092
- Key Serializer: StringSerializer
- Value Serializer: JsonSerializer
- Acks: all (for reliability)
- Retries: 3
- Compression: gzip
Consumer Configuration
java
// KafkaConsumer.java
- Bootstrap Servers: localhost:9092
- Group ID: midas-consumer-group
- Key Deserializer: StringDeserializer
- Value Deserializer: JsonDeserializer
- Auto Offset Reset: earliest
- Enable Auto Commit: false (manual commit)
Benefits of Kafka Integration
✅ Scalability - Handle millions of messages per day
✅ Fault Tolerance - Message replication and durability
✅ Decoupling - Separate data producers from consumers
✅ Real-Time Processing - Process data as it arrives
✅ Message Replay - Reprocess historical data
✅ High Throughput - Batch processing for efficiency
✅ Persistent Streaming - Store messages for configurable retention periods

💾 Database Schema
Entity: UserRecord
┌─────────────────────┐
│     UserRecord      │
├─────────────────────┤
│ user_id (PK)        │
│ username            │
│ email               │
│ first_name          │
│ last_name           │
│ balance             │
│ account_status      │
│ created_at          │
│ updated_at          │
└─────────────────────┘
Table Structure
sql
CREATE TABLE user_records (
    user_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    username VARCHAR(100) NOT NULL UNIQUE,
    email VARCHAR(150) NOT NULL UNIQUE,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    balance DECIMAL(15, 2) DEFAULT 0.00,
    account_status VARCHAR(20) DEFAULT 'ACTIVE',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_username (username),
    INDEX idx_email (email),
    INDEX idx_balance (balance)
);
Key Fields
Field	Type	Description
user_id	BIGINT	Primary key, auto-increment
username	VARCHAR(100)	Unique username
email	VARCHAR(150)	Unique email address
first_name	VARCHAR(100)	User's first name
last_name	VARCHAR(100)	User's last name
balance	DECIMAL(15,2)	Account balance with 2 decimal precision
account_status	VARCHAR(20)	Status: ACTIVE, INACTIVE, SUSPENDED
created_at	TIMESTAMP	Record creation timestamp
updated_at	TIMESTAMP	Last update timestamp
🚀 Installation & Setup
Prerequisites
Java 8 or higher installed
Apache Kafka installed and running
MySQL installed and running
Maven 3.6+ installed
Git installed
Step 1: Install Apache Kafka
Download and Setup:

bash
# Download Kafka
wget https://downloads.apache.org/kafka/3.6.0/kafka_2.13-3.6.0.tgz
tar -xzf kafka_2.13-3.6.0.tgz
cd kafka_2.13-3.6.0

# Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# In a new terminal, start Kafka broker
bin/kafka-server-start.sh config/server.properties
Create Kafka Topics:

bash
# Create user-records topic
bin/kafka-topics.sh --create \
  --topic user-records \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

# Verify topic creation
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
Step 2: Setup MySQL Database
bash
# Login to MySQL
mysql -u root -p

# Create database
CREATE DATABASE midas_db;

# Create user (optional)
CREATE USER 'midas_user'@'localhost' IDENTIFIED BY 'password';
GRANT ALL PRIVILEGES ON midas_db.* TO 'midas_user'@'localhost';
FLUSH PRIVILEGES;
exit;
Step 3: Clone the Repository
bash
git clone https://github.com/swagatika190/forage-midas.git
cd forage-midas
Step 4: Configure Application Properties
Edit src/main/resources/application.yml or application.properties:

properties
# Server Configuration
server.port=8080

# Database Configuration
spring.datasource.url=jdbc:mysql://localhost:3306/midas_db
spring.datasource.username=midas_user
spring.datasource.password=password
spring.datasource.driver-class-name=com.mysql.cj.jdbc.Driver

# JPA Configuration
spring.jpa.hibernate.ddl-auto=update
spring.jpa.show-sql=true
spring.jpa.properties.hibernate.dialect=org.hibernate.dialect.MySQL8Dialect
spring.jpa.properties.hibernate.format_sql=true

# Kafka Configuration
spring.kafka.bootstrap-servers=localhost:9092

# Kafka Producer
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JsonSerializer
spring.kafka.producer.acks=all
spring.kafka.producer.retries=3

# Kafka Consumer
spring.kafka.consumer.group-id=midas-consumer-group
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JsonDeserializer
spring.kafka.consumer.auto-offset-reset=earliest
spring.kafka.consumer.enable-auto-commit=false
spring.kafka.consumer.properties.spring.json.trusted.packages=*

# Logging
logging.level.com.jpmc.midascore=DEBUG
logging.level.org.springframework.kafka=INFO
Step 5: Build the Project
bash
mvn clean install
Step 6: Run the Application
bash
mvn spring-boot:run
The application will start on: http://localhost:8080

💡 Usage
1. Load User Data from File
Place your CSV file in the project directory:

csv
username,email,first_name,last_name,balance
john_doe,john@example.com,John,Doe,1500.00
jane_smith,jane@example.com,Jane,Smith,2500.50
The FileLoader component will automatically:

Read the file
Validate data
Publish to Kafka
Store in database
2. Monitor Kafka Messages
bash
# View messages in user-records topic
bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic user-records \
  --from-beginning
3. Query User Balance
The BalanceQuerier component provides methods to:

Query balance by user ID
Query balance by username
Get all user balances
Get users with balance above/below threshold
4. Consume Kafka Messages
The KafkaConsumer automatically:

Subscribes to user-records topic
Processes incoming messages
Updates database
Commits offsets
📁 Project Structure
forage-midas/
│
├── .mvn/wrapper/              # Maven wrapper files
├── services/                  # Service modules
├── src/
│   └── main/
│       ├── java/
│       │   └── com/jpmc/midascore/
│       │       │
│       │       ├── component/          # Reusable components
│       │       │   └── (Component classes)
│       │       │
│       │       ├── config/             # Configuration classes
│       │       │   ├── KafkaConfig.java
│       │       │   └── DatabaseConfig.java
│       │       │
│       │       ├── entity/             # Database entities
│       │       │   └── UserRecord.java
│       │       │
│       │       ├── foundation/         # Foundation/utility classes
│       │       │   └── (Base classes, utilities)
│       │       │
│       │       ├── repository/         # JPA repositories
│       │       │   └── UserRecordRepository.java
│       │       │
│       │       ├── BalanceQuerier.java      # Balance query service
│       │       ├── FileLoader.java          # File loading service
│       │       ├── KafkaConsumer.java       # Kafka consumer
│       │       ├── KafkaProducer.java       # Kafka producer
│       │       ├── UserPopulator.java       # User data populator
│       │       └── MidasCoreApplication.java # Main application
│       │
│       └── resources/
│           ├── application.properties
│           └── application.yml
│
├── .gitignore
├── application.yml
├── mvnw                       # Maven wrapper script
├── mvnw.cmd                   # Maven wrapper (Windows)
├── pom.xml                    # Maven dependencies
└── README.md                  # This file
📚 What I Learned
Building this project as part of the JPMC Forage program helped me gain expertise in:

✅ Apache Kafka:

Setting up Kafka brokers and topics
Implementing producers and consumers
Managing message serialization/deserialization
Handling offsets and consumer groups
Understanding distributed streaming architecture
✅ Event-Driven Architecture:

Designing event-driven systems
Implementing publisher-subscriber patterns
Decoupling system components
Real-time data processing
Message-driven communication
✅ Spring Boot & Kafka Integration:

Using Spring Kafka for seamless integration
Configuring Kafka producers and consumers
Implementing @KafkaListener annotations
Managing Kafka templates
Error handling in message processing
✅ Real-Time Data Processing:

Streaming large volumes of data
Processing messages in real-time
Batch vs stream processing trade-offs
Data transformation and validation
Performance optimization
✅ Financial Software Development:

Building systems for financial data
Balance management and querying
Data accuracy and consistency
Audit trails and logging
Error handling in financial transactions
✅ Software Engineering Best Practices:

Clean code architecture
Component-based design
Repository pattern implementation
Configuration externalization
Testing and debugging distributed systems
🏦 About JPMC Forage Program
J.P. Morgan Chase & Co. Advanced Software Engineering
This project was completed as part of the J.P. Morgan Chase & Co. Advanced Software Engineering Virtual Experience Program on Forage.

Program Highlights:

Simulate real-world tasks performed by JPMC software engineers
Work with enterprise-level technologies (Kafka, Spring Boot)
Learn financial software development practices
Build scalable, high-performance systems
Gain exposure to investment banking technology
Skills Developed:

Event-driven architecture design
Apache Kafka implementation
Real-time data streaming
Financial data processing
Enterprise Java development
Spring Boot application development
Certificate: https://forage-uploads-prod.s3.amazonaws.com/completion-certificates/Sj7temL583QAYpHXD/E6McHJDKsQYh79moz_Sj7temL583QAYpHXD_uujoZydahPhZeAuJs_1757880888201_completion_certificate.pdf

Learn More: Forage JPMC Program

🎯 Key Achievements
✅ Successfully implemented Kafka producer-consumer architecture
✅ Built real-time user data streaming pipeline
✅ Developed balance querying system with efficient database access
✅ Implemented file-based bulk data loading
✅ Created modular, maintainable code structure
✅ Applied Spring Boot best practices
✅ Completed JPMC Forage program requirements

🔮 Future Enhancements
 REST API - Add REST endpoints for user operations
 Authentication - Implement JWT-based security
 Transaction History - Track user transaction history
 Kafka Streams - Add real-time aggregations using Kafka Streams
 Monitoring Dashboard - Real-time monitoring UI
 Docker Support - Containerize application with Docker Compose
 Unit Tests - Comprehensive test coverage
 Microservices - Break into multiple microservices
 Cloud Deployment - Deploy on AWS/Azure with managed Kafka
🧪 Testing
Manual Testing
Test Kafka Producer:

bash
# Publish a test message
curl -X POST http://localhost:8080/api/publish \
  -H "Content-Type: application/json" \
  -d '{"username":"test_user","email":"test@example.com","balance":1000.00}'
Monitor Kafka Consumer:

bash
# Watch consumer logs
tail -f logs/application.log | grep KafkaConsumer
Query User Balance:

bash
curl http://localhost:8080/api/balance/test_user
🤝 Contributing
While this is a personal learning project, feedback and suggestions are welcome!

Fork the repository
Create a feature branch (git checkout -b feature/Improvement)
Commit changes (git commit -m 'Add improvement')
Push to branch (git push origin feature/Improvement)
Open a Pull Request
📄 License
This project is part of a virtual experience program and is for educational purposes.

👤 Contact
Swagatika Samal

💼 LinkedIn: linkedin.com/in/swagatika-samal-7762432ba
📧 Email: swagatika91575@gmail.com
🐙 GitHub: @swagatika190
🙏 Acknowledgments
J.P. Morgan Chase & Co. for providing the virtual experience program
Forage for the learning platform
Apache Kafka community for excellent documentation
Spring Boot team for the amazing framework
⭐ Show Your Support
If you found this project interesting or helpful, please give it a ⭐ star on GitHub!

Built with ❤️ by Swagatika Samal
Part of J.P. Morgan Chase & Co. Advanced Software Engineering Virtual Experience

📊 Project Statistics
Framework: Spring Boot
Message Broker: Apache Kafka
Database: MySQL
Entities: 1 (UserRecord)
Components: File Loader, Balance Querier, User Populator
Kafka Topics: 3
Program: JPMC Forage Advanced Software Engineering

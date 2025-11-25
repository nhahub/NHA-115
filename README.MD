# 🇪🇬 Egypt Air Quality Monitoring & AI Analysis

A comprehensive IoT and AI-powered air quality monitoring system for Egyptian regions, featuring real-time data simulation, Azure IoT integration, and intelligent analysis using Mistral AI. This project demonstrates a full-stack data pipeline from simulated sensor data to actionable AI-driven insights.

## 🌟 Features

### 🏭 Multi-Region Monitoring
- **8 Egyptian Regions**: Greater Cairo, Delta, Red Sea, Canal Cities, North Coast, Upper Egypt, New Valley, Sinai
- **Real-time Sensor Data**: Temperature, humidity, PM2.5, PM10, NO2, CO2
- **Smart Simulation**: Realistic environmental patterns based on sunrise/sunset and regional activities

### 🤖 AI-Powered Analysis
- **Mistral 7B Integration**: Intelligent air quality analysis and recommendations
- **Natural Language Queries**: Ask questions in plain English
- **Health Impact Assessment**: Personalized recommendations for vulnerable groups
- **Automatic Parameter Detection**: Extracts region and time period from prompts

### 📊 Interactive Dashboard
- **Power BI Integration**: Professional business intelligence dashboards
- **Gradio Web Interface**: User-friendly web application for real-time monitoring
- **Real-time Visualizations**: Interactive charts and graphs
- **Regional Comparisons**: Side-by-side analysis across Egypt
- **Export Capabilities**: Data logging and analysis reports

### ⚡ Cloud & IoT Integration
- **Azure IoT Hub**: Cloud connectivity for sensor devices
- **Azure Synapse Analytics**: Enterprise-grade data warehouse and analytics
- **Multi-device Simulation**: 8 concurrent device threads
- **Smart Data Logging**: Daily JSONL files for data persistence
- **Configurable Intervals**: Customizable data transmission rates

## 🏗️ System Architecture & Workflow

```plaintext
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────────┐
│   IoT Devices   │    │  Azure IoT Hub   │    │ Azure Stream        │
│    (8 Regions)  │──▶│                   │ ─▶│ Analytics           │
└─────────────────┘    └──────────────────┘    └─────────────────────┘
                              │                           │
                              │                           │
                              ▼                           ▼
                       ┌─────────────────┐        ┌─────────────────┐
                       │ Azure Data Lake │        │  Real-time      │
                       │    Storage      │        │     Alerts      │
                       └─────────────────┘        └─────────────────┘
                              │                           │
                              │                           │
                              ▼                           │
                       ┌─────────────────┐                │
                       │ Azure Synapse   │◀──────────────┘
                       │   Analytics     │
                       └─────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
        ▼                     ▼                     ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│   Power BI      │  │   Mistral AI    │  │    Gradio       │
│   Dashboard     │  │    Analysis     │  │   Interface     │
└─────────────────┘  └─────────────────┘  └─────────────────┘
                              │                     │
                              └─────────┬───────────┘
                                        │
                                        ▼
                               ┌─────────────────┐
                               │  Actionable     │
                               │   Insights      │
                               └─────────────────┘
```

### 🔄 Data Pipeline Workflow

1. **Data Generation & Ingestion**
   - Python scripts simulate 8 IoT devices across Egypt
   - Realistic environmental data generated every 10 seconds
   - Secure transmission to Azure IoT Hub

2. **Real-time Processing**
   - Azure Stream Analytics processes live data
   - Threshold monitoring and automatic alerts
   - Immediate anomaly detection

3. **Data Warehousing & Analytics**
   - **Azure Synapse Analytics** for scalable data storage and processing
   - Advanced SQL queries for complex data analysis
   - Data lake integration for unstructured data
   - Built-in machine learning capabilities

4. **Batch Processing & Storage**
   - ETL pipeline for historical data analysis
   - Data cleaning, validation, and aggregation
   - Storage in Azure Data Lake and Synapse Analytics

5. **AI Analysis & Insights**
   - **Mistral 7B** model processes stored data from Synapse
   - Natural language query understanding
   - Pattern recognition and predictive analytics

6. **Visualization & Reporting**
   - **Power BI** for enterprise dashboards and reporting
   - Interactive **Gradio** dashboard for real-time monitoring
   - Real-time metrics and historical trends
   - Exportable reports and recommendations

## 🔧 Azure Services Used

- **Azure IoT Hub**: Device management and data ingestion
- **Azure Stream Analytics**: Real-time data processing
- **Azure Synapse Analytics**: Data warehousing and analytics
- **Azure Data Lake Storage**: Scalable data storage
- **Power BI**: Business intelligence and visualization

## 📊 Sample Use Cases
- **Government Agencies**: Monitor regional air quality compliance

- **Healthcare Providers**: Assess environmental health risks

- **Research Institutions**: Analyze pollution patterns and trends

- **General Public**: Access real-time air quality information

### 👥 Team
- **Hassan Bassiouny**
- **Ziad Meshref**
- **Youssef Menisy**
- **Malak Abdo Ramadan**
# DEPI AI & Data Science Track Graduation Project
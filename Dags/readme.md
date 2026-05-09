# 🌊 Pakistan Flood Risk Monitoring System

An end-to-end data pipeline that fetches real-time weather data, combines it with historical flood data, calculates flood risk scores, stores results in MySQL, and visualizes them in an interactive dashboard.

## 🚀 Features

- **Automated ETL Pipeline** using Apache Airflow
- **Real-time weather data** from OpenWeatherMap API
- **Historical flood data** from FAO (Pakistan)
- **Risk scoring algorithm** based on temperature, humidity, and rainfall
- **MySQL database** for persistent storage
- **Email alerts** for high-risk cities
- **Interactive dashboard** with Streamlit (filters, charts, export)

## 🛠️ Tech Stack

- Apache Airflow 3.2.1
- Python 3.13
- MySQL 8.0
- Streamlit
- Docker & Docker Compose
- OpenWeatherMap API
- FAO Open Data

## 📋 Prerequisites

- Docker & Docker Compose installed
- OpenWeatherMap API key (free tier)
- Gmail account with App Password (for email alerts)

## 🚦 Setup & Run

### 1. Clone Repository
```bash
git clone https://github.com/yourusername/flood-risk-monitor-pakistan.git
cd flood-risk-monitor-pakistan
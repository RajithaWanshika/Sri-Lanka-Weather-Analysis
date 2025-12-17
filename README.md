# Weather Analytics

A comprehensive weather data analytics project that processes and analyzes meteorological data across multiple districts in Sri Lanka.

## Overview

The analysis focuses on understanding weather patterns, identifying climate trends, and providing actionable insights from meteorological data.

## Technologies Used

This project leverages a modern big data technology stack for distributed processing and analytics:

- **Java 8** - Core programming language for MapReduce and Spark applications
- **Apache Hadoop 3.3.1** - Distributed storage (HDFS) and MapReduce processing framework
- **Apache Spark 3.3.1** - In-memory distributed data processing engine
- **Apache Spark MLlib** - Performing Machine Learning intepretation and predictions
- **Apache Hive** - Data warehouse infrastructure for SQL-like queries on Hadoop
- **Maven** - Build automation and dependency management
- **Python 3** - Data processing scripts and dashboard server
- **Google Colabs and Jupyter** - Performing Machine Learning model
- **HTML** - Data visualization
- **Docker & Docker Compose** - Containerization for distributed cluster setup
- **HDFS** - Hadoop Distributed File System for distributed data storage
- **JUnit 5** - Unit testing framework
- **Lombok** - Java library to reduce boilerplate code

## Key Features

### 📊 District-wise Monthly Analysis
Analyzes precipitation and temperature patterns for each district on a monthly basis.

![Month-wise Precipitation & Temperature by District](resources/images/Month-wise%20Precipitation%20%26%20Temperature%20by%20District.png)

![Month-wise Precipitation & Temperature by District - Table View](resources/images/Month-wise%20Precipitation%20%26%20Temperature%20by%20District%20table%20view.png)

### 🌧️ Precipitation Analysis
Identifies periods and locations with the highest precipitation.

![Most Precipitous Analysis](resources/images/Most%20precipitous%20analysis.png)

![Top 5 Districts by Total Precipitation](resources/images/Top%205%20Districts%20by%20Total%20Precipitation.png)

![Most Precipitous Month:Season by District](resources/images/Most%20Precipitous%20Month%3ASeason%20by%20District%20table%20view.png)

### 🌡️ Temperature Distribution
Analyzes temperature patterns across different districts, providing insights into regional climate characteristics and identifying temperate regions.

![Temperature Distribution Analysis - Graph View](resources/images/Temperature%20Distribution%20Analysis%20graph%20view.png)

![Temperature Distribution Analysis - District View](resources/images/Temperature%20Distribution%20Analysis%20district%20wise%20view.png)

### 🏙️ Top Temperate Cities
Identifies cities with the most moderate and comfortable temperature ranges, useful for understanding ideal living conditions and climate zones.

### 🌱 Seasonal Evapotranspiration
Analyzes evapotranspiration patterns across different seasons, providing insights into water cycle dynamics and agricultural planning.

### ⚠️ Extreme Weather Events
Tracks and visualizes extreme weather events, helping identify patterns in severe weather occurrences.

![Extreme Weather Events](resources/images/Extreme%20Weather%20Events.png)

## Data Insights

The analytics cover:
- **Multiple Districts**: Comprehensive coverage of weather stations across Sri Lanka
- **Long-term Trends**: Analysis spanning multiple years of historical data
- **Seasonal Patterns**: Understanding of seasonal variations in weather parameters
- **Regional Comparisons**: Comparative analysis across different districts and cities

## Example Analysis: Nuwara Eliya District

Detailed analysis for specific districts like Nuwara Eliya, showing month-wise precipitation and temperature patterns.

![Nuwara Eliya District Analysis](resources/images/Month-wise%20Precipitation%20%26%20Temperature%20by%20District%20Nuwara%20Eliya.png)

## Results

The project generates comprehensive analysis results including:
- District-wise monthly weather statistics
- Highest precipitation periods and locations
- Top temperate cities rankings
- Seasonal evapotranspiration patterns
- Interactive dashboard visualizations

---

*This project processes meteorological data to provide insights into weather patterns and climate characteristics across Sri Lanka.*
